package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const (
	requesttimeout = 100
)
const (
	WORK = iota
	WAITREQUEST
	WAITPULL
)
type Shardsrequest struct{
	Num 	int
	Shard   map[string]string
	Keyshard int
}
type Shardaoly struct{
	Err			Err
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int 
	OpType   string
	Err		 Err
	Shard    map[string]string
    Keyshard int
}
type ShardinKv struct {
	state 	int
	kv		map[string]string
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	dead    	 int32
	// Your definitions here.

	sm       *shardctrler.Clerk

	config  		shardctrler.Config


	commandMap map[int64]int
	waitChMap map[int]chan Op
	kvPersist map[int]*ShardinKv


    lastIncludeIndex int          
	persister        *raft.Persister

	lock		sync.Mutex
	count       int
}
func  copyShardKV(shardinKV *ShardinKv) map[string]string {
    copiedData := make(map[string]string)
    for key, value := range shardinKV.kv {
        copiedData[key] = value
    }
    return copiedData
}
func (kv *ShardKV)Lostshards(target int,servers []string,args *Shardsrequest,apply *Shardaoly){
	for {
		for i:=0;i<len(servers);i++{
			src:=kv.make_end(servers[i])
			ok:=src.Call("ShardKV.Getshards",args,apply)
			if ok {
				if apply.Err == OK || apply.Err == ErrNewer{
					kv.mu.Lock()
					delete(kv.kvPersist,target)
					kv.mu.Unlock()
					kv.lock.Lock()
					kv.count--
					kv.lock.Unlock()
					return
				}
			}
		}
	}
}
func (kv *ShardKV)Getshards(args *Shardsrequest,apply *Shardaoly){
	if kv.killed() {
		apply.Err = ErrWrongLeader
		return
	}
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		apply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.config.Num > args.Num{
		kv.mu.Unlock()
		apply.Err = ErrNewer
		return
	}else if kv.config.Num < args.Num{
		kv.mu.Unlock()
		apply.Err = ErrOlder
		return
	}
	kv.mu.Unlock()

	op := Op{OpType:"APPEND", Shard:args.Shard,Keyshard:args.Keyshard}
	lastIndex, _, _ := kv.rf.Start(op)
	
	
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(200 * time.Millisecond)

	select {
	case <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a %vAsk :%+v,Op:%+v\n", kv.me, args.Op, args, replyOp)
		// 通过clientId、seqId确定唯一操作序列
		apply.Err = OK

	case <-timer.C:
		apply.Err = ErrWrongLeader
		kv.lock.Lock()
		kv.count--
		kv.lock.Unlock()
	}
	defer timer.Stop()
	apply.Err = OK
}
func (kv *ShardKV) dealnewshard(config shardctrler.Config){
	kv.mu.Lock()
	kv.lock.Lock()
	for key,value := range config.Shards{
		_,ok:=kv.kvPersist[key]
		if ok && value != kv.me{
			kv.count++
			kv.kvPersist[key].state = WAITREQUEST
			request := Shardsrequest{}
			apply:=Shardaoly{}
			request.Num = config.Num
			request.Keyshard = key
			request.Shard = copyShardKV(kv.kvPersist[key])
			go kv.Lostshards(key,kv.config.Groups[value],&request,&apply)
		}else if !ok && value == kv.me{
			kv.count++
			kv.kvPersist[key]=&ShardinKv{}
			if kv.config.Shards[key] == 0{
				kv.kvPersist[key].state = WORK
			}else{
				kv.kvPersist[key].state = WAITPULL
			}
		}
	}
	kv.config = config
	kv.lock.Unlock()
	kv.mu.Unlock()
}
func (kv *ShardKV) monitor() {
	for !kv.killed() {
		kv.lock.Lock()
		if kv.count == 0{
			kv.lock.Unlock()
			_, isLeader := kv.rf.GetState()
			if isLeader {
				config:= kv.sm.Query(kv.config.Num+1)
				if config.Num == kv.config.Num+1{
					kv.dealnewshard(config)
				}
			}
		}else{
			kv.lock.Unlock()
		}
		time.Sleep(requesttimeout*time.Millisecond)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return
	}
	_,isleader:=kv.rf.GetState()
	if !isleader{
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.config.Shards[key2shard(args.Key)]!=kv.me{
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	op:=Op{OpType: "Get", SeqId: args.Commandid,Key: args.Key,ClientId:args.Clientid}
	lastindex,_,_:=kv.rf.Start(op)
	op.Index=lastindex
	ch := kv.getWaitCh(lastindex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastindex)
		kv.mu.Unlock()
	}()

	timer:=time.NewTicker(100*time.Millisecond)
	defer timer.Stop()
	
	select{
	case replyOp := <-ch:
		if op.ClientId!=replyOp.ClientId ||op.SeqId!=replyOp.SeqId{
			reply.Err = ErrWrongLeader
		}else if op.Err == ErrWrongGroup{
			reply.Err = ErrWrongGroup
		}else{
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[key2shard(op.Key)].kv[op.Key]
			//DPrintf("seqID:%v get key:%v,value:%v\n",op.SeqId,args.Key,reply.Value)
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Shards[key2shard(args.Key)]!=kv.me {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	// 封装Op传到下层start
	op := Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.Commandid, ClientId: args.Clientid}
	//fmt.Printf("[ ----Server[%v]----] : send a %v,op is :%+v \n", kv.me, args.Op, op)
	lastIndex, _, _ := kv.rf.Start(op)

	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	// 设置超时ticker
	timer := time.NewTicker(200 * time.Millisecond)
	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a %vAsk :%+v,Op:%+v\n", kv.me, args.Op, args, replyOp)
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		}else if op.Err == ErrWrongGroup{
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = OK
		}

	case <-timer.C:
		reply.Err = ErrWrongLeader
	}

	defer timer.Stop()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func (kv *ShardKV) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op,1)
		ch = kv.waitChMap[index]
	}
	return ch
}
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.commandMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}
func (kv *ShardKV) isNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	len := kv.persister.RaftStateSize()
	return len >= kv.maxraftstate
}
func (kv *ShardKV) makeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e.Encode(kv.kvPersist)
	e.Encode(kv.commandMap)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}
func (kv *ShardKV)applyMsgHandlerLoop(){
	for{
		if kv.killed(){
			return
		}
		msg := <-kv.applyCh
		if msg.CommandValid{
			index:=msg.CommandIndex
			op:=msg.Command.(Op)
			kv.mu.Lock()
			if op.OpType == "APPEND" || !kv.ifDuplicate(op.ClientId, op.SeqId) {
				if kv.kvPersist[key2shard(op.Key)] == nil && (kv.kvPersist[key2shard(op.Key)].state == WAITREQUEST || kv.kvPersist[key2shard(op.Key)].state == WAITPULL){
					op.Err = ErrWrongGroup
				}else{
					switch op.OpType {
					case "Put":
						kv.kvPersist[key2shard(op.Key)].kv[op.Key] = op.Value
						//DPrintf("clientID:%v put key:%v,value:%v\n",op.ClientId,op.Key,op.Value)
					case "Append":
						//DPrintf("seqid:%v append key:%v before:%v\n",op.SeqId,op.Key,kv.kvPersist[op.Key])
						//DPrintf("clientID:%v append key:%v +op:%v\n",op.ClientId,op.Key,op.Value)
						kv.kvPersist[key2shard(op.Key)].kv[op.Key] += op.Value
						//DPrintf("seqid:%v append key:%v now:%v\n",op.SeqId,op.Key,kv.kvPersist[op.Key])
					case "APPEND":
						kv.kvPersist[op.Keyshard].kv = op.Shard	
						kv.kvPersist[op.Keyshard].state = WORK		
					}
				}
				kv.commandMap[op.ClientId] = op.SeqId
                if kv.isNeedSnapshot(){
                    go kv.makeSnapshot(msg.CommandIndex)
                }
			}
			kv.mu.Unlock()
			if _, isLead := kv.rf.GetState(); isLead {
				kv.getWaitCh(index) <- op
			}
		}else if msg.SnapshotValid{
            kv.decodeSnapshot(msg.SnapshotIndex, msg.Snapshot)
        }
	}
}

func (kv *ShardKV) decodeSnapshot(index int, snapshot []byte) {

	// 这里必须判空，因为当节点第一次启动时，持久化数据为空，如果还强制读取数据会报错
	if len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastIncludeIndex = index

	if d.Decode(&kv.kvPersist) != nil || d.Decode(&kv.commandMap) != nil {
		panic("error in parsing snapshot")
	}
	//DPrintf("----- install\n")

}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.commandMap = make(map[int64]int)
	kv.kvPersist = make(map[int]*ShardinKv,shardctrler.NShards)
	kv.waitChMap = make(map[int]chan Op)

    kv.persister = persister
    kv.lastIncludeIndex = 0
	kv.mu = sync.Mutex{}

	kv.sm=shardctrler.MakeClerk(kv.ctrlers)
	kv.config = shardctrler.Config{}
	kv.config.Groups = map[int][]string{}
	kv.config.Num = 0

	kv.count = 0
	kv.lock = sync.Mutex{}

	kv.decodeSnapshot(kv.rf.GetFirstLog().Index,kv.persister.ReadSnapshot())
	go kv.applyMsgHandlerLoop()
	go kv.monitor()
	return kv
}
