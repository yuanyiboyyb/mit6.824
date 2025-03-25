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
	requesttimeout = 20
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
const(
	MONITOR = iota
	SHARD
)
type Shardsrequest struct{
	Num 	int
	Shard   map[string]string
	Keyshard int
	CommandMap map[int64]int
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
	CommandMap map[int64]int
}
type ShardinKv struct {
	State 	int
	Kv		map[string]string
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
	oldconfig 		shardctrler.Config


	commandMap map[int64]int
	waitChMap map[int]chan Op
	kvPersist map[int]*ShardinKv


    lastIncludeIndex int          
	persister        *raft.Persister

	State 			int

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
	_, isLeader := kv.rf.GetState()
	if debugMode && debugLogger != nil && isLeader {  
		debugLogger.Printf("Gid:%v Id:%v "+format+"\n",append([]interface{}{kv.gid,kv.me}, args...)...)
	}
}










/* func  copyShardKV(shardinKV *ShardinKv) map[string]string {
    copiedData := make(map[string]string)
    for key, value := range shardinKV.kv {
        copiedData[key] = value
    }
    return copiedData
} */
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
			if apply.Err == OK{
				kv.mu.Lock()
				kv.kvPersist[target].State = READY
				kv.mu.Unlock()
				kv.DPrintf("SHARD %v push success",target)
				return
			}else if apply.Err == Errnew{
				time.Sleep(1000*time.Millisecond)
			}else if apply.Err == Errold{
				return
			}else{
				i=(i+1)%len(servers)	
			}
		}else{
			i=(i+1)%len(servers)
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
	if kv.config.Num < args.Num{
		kv.mu.Unlock()
		apply.Err = Errnew
		return
	}
	if kv.config.Num > args.Num{
		kv.mu.Unlock()
		apply.Err = Errold
		return
	}
	if kv.State !=SHARD{
		kv.mu.Unlock()
		apply.Err = Errold
		return
	}
	kv.mu.Unlock()

	op := Op{OpType:OPPUSH, Shard:args.Shard,Keyshard:args.Keyshard,SeqId:args.Num,CommandMap: args.CommandMap}
	lastIndex, _, _ := kv.rf.Start(op)
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, op.Index)
		kv.mu.Unlock()
	}()

	timer := time.NewTicker(500 * time.Millisecond)

	select {
	case replyop:=<-ch:
		if replyop.Err == OK{
			apply.Err = OK
			kv.DPrintf("shard %v pull success",args.Keyshard)
			return
		}else{
			apply.Err = replyop.Err
		}
	case <-timer.C:
		apply.Err = ErrWrongLeader
	}
	defer timer.Stop()
}
func (kv *ShardKV) dealnewshard(){
	if kv.killed() {
		return 
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader{
		return 
	}
	kv.mu.Lock()
	num:=kv.config.Num
	for key,value:=range kv.kvPersist{
		if value.State == WAITPULL{
			args:=Shardsrequest{Num: num,Shard: value.Kv,Keyshard: key,CommandMap: kv.commandMap}
			kv.DPrintf("------- %v %v %v %v",key,value.Kv,kv.config.Groups[kv.config.Shards[key]],kv.oldconfig.Groups[kv.config.Shards[key]])
			apply:=Shardaoly{}
			if _,ok:=kv.config.Groups[kv.config.Shards[key]];ok{
				go kv.Lostshards(key,kv.config.Groups[kv.config.Shards[key]],&args,&apply)
			}else{
				go kv.Lostshards(key,kv.oldconfig.Groups[kv.config.Shards[key]],&args,&apply)
			}
		}
	}
	kv.mu.Unlock()
	for {
		time.Sleep(100*time.Millisecond)
		if kv.killed() {
			return 
		}
		_, isLeader := kv.rf.GetState()
		if !isLeader{
			return 
		}
		flag:=true
		kv.mu.Lock()
		for _,value:=range kv.kvPersist{
			if value.State == WAITPULL || value.State == WAITPUSH{
				flag = false
			}
		}
		kv.mu.Unlock()
		if flag {
			kv.DPrintf("ALL SHARD DEALED")
			for{
				if kv.killed() {
					return
				}
				_, isLeader := kv.rf.GetState()
				if !isLeader{
					return
				}
				op := Op{OpType: OPSHARD,SeqId:num}
				lastindex,_,_:=kv.rf.Start(op)
				ch := kv.getWaitCh(lastindex)
				defer func() {
					kv.mu.Lock()
					delete(kv.waitChMap, op.Index)
					kv.mu.Unlock()
				}()
		
				timer:=time.NewTicker(500*time.Millisecond)
				defer timer.Stop()
		
				select{
					case <-ch:
						return
					case <-timer.C:
				}
			}
		}
	}
}
func (kv *ShardKV)Applyconfig(config shardctrler.Config){
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
	defer func(){
		kv.mu.Lock()
		delete(kv.waitChMap, lastindex)
		kv.mu.Unlock()
	}()
	
	timer:=time.NewTicker(500*time.Millisecond)
	defer timer.Stop()

	select{
	case <-ch:
	case <-timer.C:
	}
}
func (kv *ShardKV) monitor(){
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			switch kv.State{
			case MONITOR:
				config:= kv.sm.Query(kv.config.Num+1)
				kv.mu.Lock()
				if config.Num == kv.config.Num+1{
					kv.mu.Unlock()
					kv.DPrintf("START NEW CONFIG %v",config.Num)
					kv.Applyconfig(config)
				}else{
					kv.mu.Unlock()
				}
			case SHARD:
				kv.DPrintf("START DEAL SHARES")
				kv.dealnewshard()
			}
		}
		time.Sleep(200*time.Millisecond)
	}
}
/* func (kv *ShardKV) monitor() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			config:= kv.sm.Query(kv.config.Num+1)
			kv.mu.Lock()
			if config.Num == kv.config.Num+1{
				kv.mu.Unlock()
				kv.DPrintf("START NEW CONFIG")
				kv.Applyconfig(config)
				kv.dealnewshard()
			}else{
				kv.mu.Unlock()
			}
		}
		time.Sleep(requesttimeout*time.Millisecond)
	}
} */


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
	if args.Num < kv.config.Num{
		kv.mu.Unlock()
		reply.Err = Errold
		return
	}
	if args.Num > kv.config.Num{
		kv.mu.Unlock()
		reply.Err = Errnew
		return
	}
	if kv.config.Shards[key2shard(args.Key)]!=kv.gid{
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()
	kv.DPrintf("START GET")
	op:=Op{OpType: "Get", SeqId: args.Commandid,Key: args.Key,ClientId:args.Clientid}
	lastindex,_,_:=kv.rf.Start(op)
	op.Index=lastindex
	ch := kv.getWaitCh(lastindex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastindex)
		kv.mu.Unlock()
	}()

	timer:=time.NewTicker(4000*time.Millisecond)
	defer timer.Stop()
	
	select{
	case replyOp := <-ch:
		if op.ClientId!=replyOp.ClientId ||op.SeqId!=replyOp.SeqId{
			reply.Err = ErrWrongLeader
		}else if replyOp.Err == ErrWrongGroup{
			reply.Err = ErrWrongGroup
		}else if replyOp.Err == ErrWait{
			reply.Err = ErrWait
		}else if replyOp.Err == ErrFail{
			kv.DPrintf("111111111")
			reply.Err = ErrFail
		}else {
			reply.Err = OK
			reply.Value = replyOp.Value
			kv.DPrintf("GET FINISH %v %v %v %v",args.Key,key2shard(args.Key),replyOp.Value,reply.Value)
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
	if args.Num < kv.config.Num{
		kv.mu.Unlock()
		kv.DPrintf("1111111111111")
		reply.Err = Errold
		return
	}
	if args.Num > kv.config.Num{
		kv.mu.Unlock()
		kv.DPrintf("22222222")
		reply.Err = Errnew
		return
	}
	if kv.config.Shards[key2shard(args.Key)]!=kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}
	kv.mu.Unlock()

	// 封装Op传到下层start
	kv.DPrintf("START PutAppend")
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
	timer := time.NewTicker(3000 * time.Millisecond)
	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a %vAsk :%+v,Op:%+v\n", kv.me, args.Op, args, replyOp)
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		}else if replyOp.Err == ErrWrongGroup{
			reply.Err = ErrWrongGroup
		}else if replyOp.Err == ErrWait{
			reply.Err = ErrWait
		}else if replyOp.Err == OK{
			reply.Err = OK
			kv.DPrintf("FINISH PutAppend %v",replyOp.Err==OK)
		}else{
			reply.Err = replyOp.Err
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,value:=range kv.kvPersist{
		if value.State!=NONE && value.State!=WORK{
			return
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvPersist)
	e.Encode(kv.commandMap)
	e.Encode(kv.config)
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
			if op.OpType == OPCONFIG {
				if op.Config.Num > kv.config.Num{
					for key,value:=range kv.kvPersist{
						if op.Config.Shards[key] == kv.gid && kv.config.Shards[key] == 0{
							value.State = READY
						}else if op.Config.Shards[key] != kv.gid && kv.config.Shards[key] == kv.gid{
							value.State = WAITPULL
							kv.DPrintf("%v need to give %v",key,op.Config.Shards[key])
						}else if op.Config.Shards[key] == kv.gid && kv.config.Shards[key]!=kv.gid{
							value.State = WAITPUSH
							kv.DPrintf("%v need to be given by %v",key, kv.config.Shards[key])
						}
					}
					kv.oldconfig = kv.config
					kv.config  = op.Config
					kv.State = SHARD
					kv.DPrintf("NEW CONFIG APPLIED %v",kv.config.Num)
				}
			}else if op.OpType == OPSHARD {
				if  op.SeqId == kv.config.Num{
					for key,value:=range kv.config.Shards{
						if value!=kv.gid{
							kv.kvPersist[key].State = NONE
							kv.kvPersist[key].Kv = make(map[string]string)
						}else{
							kv.kvPersist[key].State = WORK
						}
					}
					kv.State = MONITOR
					kv.DPrintf("ALL FINISH")
				}
			}else if op.OpType == OPPUSH {
				if op.SeqId == kv.config.Num{
					if kv.kvPersist[op.Keyshard].State == WAITPUSH{
						kv.DPrintf("SHARE:%v pull success new:%v old:%v",op.Keyshard,op.Shard,kv.kvPersist[op.Keyshard].Kv)
						kv.kvPersist[op.Keyshard].Kv = op.Shard
						kv.kvPersist[op.Keyshard].State = READY
						for key,value:= range op.CommandMap{
							kv.commandMap[key] = max(value,kv.commandMap[key])
						}
						op.Err = OK
					}else{
						op.Err = Errold
					}
				}else{
					op.Err = 	ErrFail
				}
			}else{
				if kv.kvPersist[key2shard(op.Key)].State == WAITPUSH ||kv.kvPersist[key2shard(op.Key)].State == READY{
					op.Err = ErrWait
					kv.DPrintf("ERRWAIT:%v %v",op.Key,key2shard(op.Key))
				}else if kv.kvPersist[key2shard(op.Key)].State == WAITPULL ||kv.kvPersist[key2shard(op.Key)].State == NONE{
					op.Err = ErrWrongGroup
					kv.DPrintf("ERRWRONGGROUP:%v %v %v",op.Key,key2shard(op.Key))
				}else{
					if !kv.ifDuplicate(op.ClientId, op.SeqId) {
						switch op.OpType {
						case "Put":
							kv.kvPersist[key2shard(op.Key)].Kv[op.Key] = op.Value
						case "Append":
							kv.kvPersist[key2shard(op.Key)].Kv[op.Key] += op.Value
						case "Get":
							op.Value = kv.kvPersist[key2shard(op.Key)].Kv[op.Key]
							kv.DPrintf("op.value:%v",op.Value)
						}
						kv.DPrintf("kv.kvpersist[%v].kv:%v",key2shard(op.Key),kv.kvPersist[key2shard(op.Key)].Kv)
						op.Err = OK
						kv.commandMap[op.ClientId] = op.SeqId
					}else{
						op.Err = ErrFail
					}
				}
			}
			kv.mu.Unlock()
			kv.getWaitCh(index)<-op
			if kv.isNeedSnapshot(){
				go kv.makeSnapshot(msg.CommandIndex)
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

	if d.Decode(&kv.kvPersist) != nil || d.Decode(&kv.commandMap) != nil || d.Decode(&kv.config) != nil{
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
            State: NONE,
            Kv:    make(map[string]string),
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
	kv.oldconfig = shardctrler.Config{}
	kv.oldconfig.Groups = map[int][]string{}
	kv.oldconfig.Num = 0

	kv.State = MONITOR

	kv.decodeSnapshot(kv.rf.GetFirstLog().Index,kv.persister.ReadSnapshot())

	kv.DPrintf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	go kv.applyMsgHandlerLoop()
	go kv.monitor()
	return kv
}
