package kvraft

import (
	"bytes"
	/* "fmt"
	"log"
	"os" */
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
/* func init() {
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
} */
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


    lastIncludeIndex int          
	persister        *raft.Persister
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
func (kv *KVServer) getWaitCh1(index int) (chan Op, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, exist := kv.waitChMap[index]
	if !exist {
		return nil,false
	}
	return ch,true
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
			//DPrintf("seqID:%v get key:%v,value:%v\n",op.SeqId,args.Key,reply.Value)
			kv.mu.Unlock()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
	}
}
func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {

	lastSeqId, exist := kv.commandMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}
func (kv *KVServer) isNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	len := kv.persister.RaftStateSize()
	return len >= kv.maxraftstate
}
func (kv *KVServer) makeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e.Encode(kv.kvPersist)
	e.Encode(kv.commandMap)
	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}
func (kv *KVServer) decodeSnapshot(index int, snapshot []byte) {

	// 这里必须判空，因为当节点第一次启动时，持久化数据为空，如果还强制读取数据会报错
	if snapshot == nil || len(snapshot) < 1 {
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
func (kv *KVServer)applyMsgHandlerLoop(){
	for{
		if kv.killed(){
			return
		}
		msg := <-kv.applyCh
		if msg.CommandValid{
			index:=msg.CommandIndex
			op:=msg.Command.(Op)
			kv.mu.Lock()
			if !kv.ifDuplicate(op.ClientId, op.SeqId) {
				switch op.OpType {
				case "Put":
					kv.kvPersist[op.Key] = op.Value
					//DPrintf("clientID:%v put key:%v,value:%v\n",op.ClientId,op.Key,op.Value)
				case "Append":
					//DPrintf("seqid:%v append key:%v before:%v\n",op.SeqId,op.Key,kv.kvPersist[op.Key])
					//DPrintf("clientID:%v append key:%v +op:%v\n",op.ClientId,op.Key,op.Value)
					kv.kvPersist[op.Key] += op.Value
					//DPrintf("seqid:%v append key:%v now:%v\n",op.SeqId,op.Key,kv.kvPersist[op.Key])
				}
				kv.commandMap[op.ClientId] = op.SeqId
                if kv.isNeedSnapshot(){
                    go kv.makeSnapshot(msg.CommandIndex)
                }
			}
			kv.mu.Unlock()
			if _, isLead := kv.rf.GetState(); isLead {
				th,ok:=kv.getWaitCh1(index)
				if ok {
					th <- op
				}
			}
		}else if msg.SnapshotValid{
            kv.decodeSnapshot(msg.SnapshotIndex, msg.Snapshot)
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
	timer := time.NewTicker(200 * time.Millisecond)
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

	kv.applyCh =  make(chan raft.ApplyMsg, 1200)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.commandMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

    kv.persister = persister
    kv.lastIncludeIndex = 0
	kv.mu = sync.Mutex{}

	kv.decodeSnapshot(kv.rf.GetFirstLog().Index,kv.persister.ReadSnapshot())
	go kv.applyMsgHandlerLoop()
	return kv
}
