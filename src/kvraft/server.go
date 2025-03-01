package kvraft

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

/* const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}  */ 
func init() {
    if debugMode {
	    logFile, err := os.OpenFile("debug1.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
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
func DPrintf(format string,args ...interface{}) {
	if debugMode && debugLogger != nil {
		debugLogger.Printf(format, args...)
	}
}
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// otherwise RPC will break.
	SeqId    int
	Key      string
	Value    string
	ClientId int64
	Index    int // raft服务层传来的Index
	OpType   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commandMap map[int64]int
	waitChMap map[int]chan Op
	kvPersist map[string]string 
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan Op,1)
		ch = kv.waitChMap[index]
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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
			reply.Err=ErrWrongLeader
		}else{
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvPersist[args.Key] 
			DPrintf("seqID:%v get key:%v,value:%v\n",op.SeqId,args.Key,reply.Value)
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}
func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.commandMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer)applyMsgHandlerLoop(){
	for{
		if kv.killed(){
			return
		}
		msg := <-kv.applyCh
		if msg.CommandValid{
			index:=msg.CommandIndex
			op:=msg.Command.(Op)
			if !kv.ifDuplicate(op.ClientId, op.SeqId) {
				kv.mu.Lock()
				switch op.OpType {
				case "Put":
					kv.kvPersist[op.Key] = op.Value
					DPrintf("seqID:%v put key:%v,value:%v\n",op.SeqId,op.Key,op.Value)
				case "Append":
					kv.kvPersist[op.Key] += op.Value
					DPrintf("sedID:%v append key:%v,value:%v\n",op.SeqId,op.Key,kv.kvPersist[op.Key])
				}
				kv.commandMap[op.ClientId] = op.SeqId
				kv.mu.Unlock()
			}
			if _, isLead := kv.rf.GetState(); isLead {
				kv.getWaitCh(index) <- op
			}
  
		}
	}
}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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
	timer := time.NewTicker(150 * time.Millisecond)
	select {
	case replyOp := <-ch:
		//fmt.Printf("[ ----Server[%v]----] : receive a %vAsk :%+v,Op:%+v\n", kv.me, args.Op, args, replyOp)
		// 通过clientId、seqId确定唯一操作序列
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}

	case <-timer.C:
		DPrintf("111111\n")
		reply.Err = ErrWrongLeader
	}

	defer timer.Stop()
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh =  make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.commandMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)
	kv.mu = sync.Mutex{}

	for i:=1;i<5;i++{
		go kv.applyMsgHandlerLoop()
	}
	return kv
}
