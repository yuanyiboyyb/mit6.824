package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"os"
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
	OPPUT = "Put"
	OPAPPEND = "Append"
	OPCONFIG = "Config"
	OPSHARD  = "Shard"
	OPPUSH   ="Push"
)
const (
	NONE = iota
	READY
	WORK 
	WAITPUSH
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
	Config   shardctrler.Config
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

}

func init() {
    if debugMode {
	    logFile, err := os.OpenFile("debug.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	    if err != nil {
		    fmt.Println("无法打开日志文件:", err)
		    return
	    }
	    // 创建 Logger
	    debugLogger = log.New(logFile, "[DEBUG] ", log.Lshortfile)
    }
} 
const (
    debugMode = true
)
var  debugLogger *log.Logger
func (kv *ShardKV)DPrintf(format string,args ...interface{}) {
	if debugMode && debugLogger != nil {  
		debugLogger.Printf("ID:%v   "+format+"\n",append([]interface{}{kv.gid}, args...)...)
	}
}










func  copyShardKV(shardinKV *ShardinKv) map[string]string {
    copiedData := make(map[string]string)
    for key, value := range shardinKV.kv {
        copiedData[key] = value
    }
    return copiedData
}
func (kv *ShardKV)Lostshards(target int,servers []string,args *Shardsrequest,apply *Shardaoly){
	i:=0
	for{
		if kv.killed() {
			return
		}
		_, isLeader := kv.rf.GetState()
		if !isLeader{
			return
		}
		src:=kv.make_end(servers[i])
		ok:=src.Call("ShardKV.Getshards",args,apply)
		if ok {
			if apply.Err == OK || apply.Err == ErrNewer{
				kv.mu.Lock()
				kv.kvPersist[target].state = READY
				kv.mu.Unlock()
				return
			}else if apply.Err != ErrOlder{
				i=(i+1)%len(servers)
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

	op := Op{OpType:OPPUSH, Shard:args.Shard,Keyshard:args.Keyshard}
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(200 * time.Millisecond)

	select {
	case <-ch:
		apply.Err = OK
	case <-timer.C:
		apply.Err = ErrWrongLeader
	}
	defer timer.Stop()
}
func (kv *ShardKV) dealnewshard(){
	for{
		if kv.killed() {
			return
		}
		_, isLeader := kv.rf.GetState()
		if !isLeader{
			return
		}
		flag:=false
		kv.mu.Lock()
		for key,value:=range kv.kvPersist{
			if value.state == WAITPULL{
				flag = true
			}else if value.state == WAITPUSH{
				flag = true
				args:=Shardsrequest{Num: kv.config.Num,Shard: kv.kvPersist[key].kv,Keyshard: key}
				apply:=Shardaoly{}
				go kv.Lostshards(key,kv.config.Groups[kv.config.Shards[key]],&args,&apply)
			}
		}
		kv.mu.Unlock()
		if flag == false{
			for{
				if kv.killed() {
					return
				}
				_, isLeader := kv.rf.GetState()
				if !isLeader{
					return
				}
				op := Op{OpType: OPSHARD}
				lastindex,_,_:=kv.rf.Start(op)
				ch := kv.getWaitCh(lastindex)

				timer:=time.NewTicker(100*time.Millisecond)
				defer timer.Stop()

				select{
				case replyOp := <-ch:
					if replyOp.Err == OK{
						kv.mu.Lock()
						delete(kv.waitChMap, lastindex)
						kv.mu.Unlock()
						return
					}
				case <-timer.C:
				}
				kv.mu.Lock()
				delete(kv.waitChMap, lastindex)
				kv.mu.Unlock()
			}
		}
	}
}
func (kv *ShardKV)Applyconfig(config shardctrler.Config){
	for{
		if kv.killed() {
			return
		}
		_, isLeader := kv.rf.GetState()
		if !isLeader{
			return
		}
		op := Op{OpType: OPCONFIG,Config:config}
		lastindex,_,_:=kv.rf.Start(op)
		op.Index=lastindex
		ch := kv.getWaitCh(lastindex)
	
		timer:=time.NewTicker(100*time.Millisecond)
		defer timer.Stop()

		select{
		case replyOp := <-ch:
			if replyOp.Err == OK{
				kv.mu.Lock()
				delete(kv.waitChMap, lastindex)
				kv.mu.Unlock()
				return
			}
		case <-timer.C:
		}
		kv.mu.Lock()
		delete(kv.waitChMap, lastindex)
		kv.mu.Unlock()
	}
}
func (kv *ShardKV) monitor() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			config:= kv.sm.Query(kv.config.Num+1)
			if config.Num == kv.config.Num+1{
				kv.Applyconfig(config)
				kv.dealnewshard()
			}
		}
	}
	time.Sleep(requesttimeout*time.Millisecond)
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
	if kv.config.Shards[key2shard(args.Key)]!=kv.gid{
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
		}else if replyOp.Err == ErrWrongGroup{
			reply.Err = ErrWrongGroup
		}else if replyOp.Err == ErrWait{
			reply.Err = ErrWait
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
	if kv.config.Shards[key2shard(args.Key)]!=kv.gid {
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
		}else if op.Err == ErrWait{
			reply.Err = ErrWait
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
			if op.OpType == OPCONFIG{
				if op.Config.Num > kv.config.Num{
					for key,value:=range kv.kvPersist{
						if op.Config.Shards[key] != kv.gid && kv.config.Shards[key] == kv.gid{
							value.state = WAITPULL
						}else if op.Config.Shards[key] == kv.gid && kv.config.Shards[key]!=kv.gid{
							value.state = WAITPUSH
						}
					}
				}
				op.Err = OK
			}else if op.OpType == OPSHARD{
				for _,value:=range kv.kvPersist{
					if value.state == WAITPUSH{
						value.state = WORK
					}else if value.state == WAITPULL{
						value.state = NONE
						value.kv = make(map[string]string)
					}
				}
				op.Err = OK
			}else if op.OpType == OPPUSH{
				kv.kvPersist[op.Keyshard].kv = op.Shard
				kv.kvPersist[op.Keyshard].state = READY
				op.Err = OK
			}else{
				if kv.kvPersist[key2shard(op.Key)].state!=WORK{
					op.Err = ErrWait
				}else{
					switch op.OpType {
					case "Put":
						kv.kvPersist[key2shard(op.Key)].kv[op.Key] = op.Value
					case "Append":
						kv.kvPersist[key2shard(op.Key)].kv[op.Key] += op.Value
					}
					op.Err = OK
				}
			}
			kv.commandMap[op.ClientId] = op.SeqId
			if kv.isNeedSnapshot(){
				go kv.makeSnapshot(msg.CommandIndex)
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
	for i := range shardctrler.NShards {
        kv.kvPersist[i] = &ShardinKv{
            state: NONE,
            kv:    make(map[string]string),
        }
    }
	
	kv.waitChMap = make(map[int]chan Op)

    kv.persister = persister
    kv.lastIncludeIndex = 0
	kv.mu = sync.Mutex{}

	kv.sm=shardctrler.MakeClerk(kv.ctrlers)
	kv.config = shardctrler.Config{}
	kv.config.Groups = map[int][]string{}
	kv.config.Num = 0


	kv.decodeSnapshot(kv.rf.GetFirstLog().Index,kv.persister.ReadSnapshot())


	go kv.applyMsgHandlerLoop()
	go kv.monitor()
	return kv
}
