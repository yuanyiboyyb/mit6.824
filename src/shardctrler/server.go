package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead 	int32

	waiCh map[int]chan Result
	historyMap map[int64] Result

	// Your data here.

	configs []Config // indexed by config num
	gidshares   map[int][]int
}

type OType string


const (
	OPJoin  OType = "Join"
	OPLeave OType = "Leave"
	OPMove  OType = "Move"
	OPQuery OType = "Query"
)
type Result struct{
	LastSeq int64
	Config  Config
	Err		Err
	Id 		int64
}
type Op struct {
	// Your data here.
	OpType		OType
	Servers		map[int][]string
	GIDs		[]int
	Shard       int
	GID			int
	Num         int

	Seq			int64
	Id 			int64
}
func (sc * ShardCtrler)HandleOp(opargs Op)(res Result){
	_, ifLeader := sc.rf.GetState()
	if !ifLeader{
		return Result{Err: ErrWrongLeader}
	}
	startIndex,_,_:=sc.rf.Start(opargs)
	newch:=sc.getWaitCh(startIndex)
	defer func() {
		sc.mu.Lock()
		delete(sc.waiCh, startIndex)
		close(newch)
		sc.mu.Unlock()
	}()

	timer:=time.NewTicker(100*time.Millisecond)
	defer timer.Stop()
	select{
	case<-timer.C:
		res.Err = ErrWrongLeader
		return
	case msg := <-newch:
		if msg.LastSeq == opargs.Seq && msg.Id == opargs.Id{
			res = msg
			return
		}else {
			res.Err = ErrWrongLeader
			return
		}
	}
}
func (sc *ShardCtrler) getWaitCh(index int) chan Result {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, exist := sc.waiCh[index]
	if !exist {
		sc.waiCh[index] = make(chan Result,1)
		ch = sc.waiCh[index]
	}
	return ch
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *MyReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	opArgs := Op{OpType: OPJoin,Servers: args.Servers,Seq: args.Seq,Id: args.Id}
	res:=sc.HandleOp(opArgs)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *MyReply) {
	// Your code here.
	if sc.killed() {
		reply.Err =  ErrWrongLeader
		return
	}
	opArgs := Op{OpType: OPLeave, Seq: args.Seq, Id: args.Id, GIDs: args.GIDs}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MyReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	opArgs := Op{OpType: OPMove, Seq: args.Seq, Id: args.Id, Shard: args.Shard, GID: args.GID}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *MyReply) {
	// Your code here.
	if sc.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	opArgs := Op{OpType: OPMove, Seq: args.Seq, Id: args.Id}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Config = res.Config
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)

}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) getaverage(gids []int,need []int) [NShards]int{
	var shared [NShards]int
	shared=sc.configs[len(sc.configs)-1].Shards
	average:=NShards/len(gids)
	over:=NShards%len(gids)
	var num int
	var change int
	var flag int
	for _,gid :=range(gids){
		if over > 0{
			flag = 1
			over-=1
		}else{
			flag = 0
		}
		if len(sc.gidshares) > average+flag{
			need=append(need,sc.gidshares[gid][average+1:]...)
			sc.gidshares[gid]=sc.gidshares[gid][:average+1]
		}else{
			num = average+1-len(sc.gidshares[gid])
			for _,change=range(need[len(need)-num:]){
				shared[change]=gid
			}
			sc.gidshares[gid]=append(sc.gidshares[gid], need[len(need)-num:]...)
		}
	}
	return shared
}

func(sc *ShardCtrler)CreateNewConfig(Servers map[int][]string)Config{
	newconfig := Config{}
	gids := make([]int, 0)
	for key,value:=range sc.configs[len(sc.configs)-1].Groups{
		newconfig.Groups[key]=value
		gids = append(gids, key)
	}
	for key,value := range Servers{
		newconfig.Groups[key] = value
		sc.gidshares[key] = make([]int,0)
		gids=append(gids, key)
	}

	sort.Slice(gids,func(i,j int) bool{
		return len(sc.gidshares[gids[i]])>len(sc.gidshares[gids[j]])
	})
	need:=make([]int,0)
	newconfig.Shards = sc.getaverage(gids,need)
	newconfig.Num = sc.configs[len(sc.configs)-1].Num+1
	return newconfig
}
func(sc *ShardCtrler)RemoveGidServers(Gids []int)Config{
	newconfig := Config{}
	gids:=make([]int,0)
	need:=make([]int,0)
	sort.Slice(Gids,func(i,j int) bool{
		return Gids[i]<Gids[j]
	})
	i:=0
	for key,value :=range sc.configs[len(sc.configs)-1].Groups{
		if Gids[i] == key{
			i++
			need=append(need, sc.gidshares[key]...)
			delete(sc.gidshares,key)
		}else{
			newconfig.Groups[key]=value
			gids=append(gids, key)
		}
	}
	sort.Slice(gids,func(i,j int) bool{
		return len(sc.gidshares[gids[i]])>len(sc.gidshares[gids[j]])
	})
	newconfig.Shards = sc.getaverage(gids,need)
	newconfig.Num = sc.configs[len(sc.configs)-1].Num+1
	return newconfig
}

func(sc *ShardCtrler)MoveShard2Gid(shared int,gid int)Config{
	newconfig := Config{}
	Bgid:=sc.configs[len(sc.configs)-1].Shards[shared]
	for index,value := range sc.gidshares[Bgid]{
		if value == shared{
			sc.gidshares[Bgid]=append(sc.gidshares[Bgid][:index],sc.gidshares[Bgid][index+1:]... )
			sc.gidshares[gid]=append(sc.gidshares[gid], shared)
			break
		}
	}
	newconfig.Num = sc.configs[len(sc.configs)-1].Num+1
	for key,value := range sc.gidshares{
		for _,share := range value{
			newconfig.Shards[share] = key
		} 
	}
	newconfig.Groups = sc.configs[len(sc.configs)-1].Groups
	return newconfig
}
func(sc *ShardCtrler)QueryConfig(num int)Config{
	if num == -1{
		return sc.configs[len(sc.configs)-1]
	}else{
		return sc.configs[num]
	}
}
func (sc *ShardCtrler) ConfigExecute(op *Op) (res Result) {
	// 调用时要求持有锁
	res.LastSeq = op.Seq
	switch op.OpType {
	case OPJoin:
		newConfig := sc.CreateNewConfig(op.Servers)
		sc.configs=append(sc.configs, newConfig)
		res.Err = OK
	case OPLeave:
		newConfig := sc.RemoveGidServers(op.GIDs)
		sc.configs=append(sc.configs, newConfig)
		res.Err = OK
	case OPMove:
		newConfig := sc.MoveShard2Gid(op.Shard, op.GID)
		sc.configs=append(sc.configs, newConfig)
		res.Err = OK
	case OPQuery:
		rConfig := sc.QueryConfig(op.Num)
		res.Config = rConfig
		res.Err = OK
	}
	return
}
func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int64) bool {

	lastResult, exist := sc.historyMap[clientId]
	if !exist {
		return false
	}
	return seqId <= lastResult.LastSeq
}
func (sc *ShardCtrler) ApplyHandler() {
	for !sc.killed() {
		log := <-sc.applyCh
		index:=log.CommandIndex
		if log.CommandValid {
			op := log.Command.(Op)
			sc.mu.Lock()
			res := Result{}
			if !sc.ifDuplicate(op.Id,op.Seq){
				res = sc.ConfigExecute(&op)
				sc.historyMap[op.Id] = res
			}
			sc.mu.Unlock()
			if _, isLead := sc.rf.GetState(); isLead {
				sc.getWaitCh(index) <- res
			}
		}	
	}
}//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.historyMap = make(map[int64]Result)
	sc.waiCh = make(map[int]chan Result)
	sc.gidshares=make(map[int][]int,0)
	return sc
}
