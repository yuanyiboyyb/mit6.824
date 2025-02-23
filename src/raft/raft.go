package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"bytes"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

type InstallSnapshotArgs struct {
	Term              int    // 是发送快照的领导者当前的任期。
	LeaderId          int    // 是领导者的标识符，它帮助跟随者将客户端请求重定向到当前的领导者。
	LastIncludedIndex int    // 是快照中包含的最后一个日志索引。这个索引及其之前的所有日志条目都将被快照替换。
	LastIncludedTerm  int    // LastIncludedIndex 所属的任期。它确保快照包含最新的信息。
	Data              []byte // Data 是表示快照数据的原始字节切片。快照以分块的方式发送，从指定的偏移量开始。
}

// InstallSnapshotReply 定义了跟随者对快照安装请求的响应结构。
type InstallSnapshotReply struct {
	// Term 是跟随者当前的任期。领导者接收到此响应后会检查该任期，以确认自己是否仍然是当前的领导者。
	Term int
}
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type AppendEntriesArgs struct{
    Term         int          // 领导者的当前任期
	LeaderId     int          // 领导者的 ID
	PrevLogIndex int          // 新日志条目之前的日志条目的索引
	PrevLogTerm  int          // 新日志条目之前的日志条目的任期
	Entries      []Logentries // 要存储的日志条目（为空表示心跳，可以发送多个以提高效率） log entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int          // 领导者的 commitIndex

}
type AppendEntriesReply struct{
    Term          int  // 当前任期，用于领导者更新自己的任期
	Success       bool // 收到回复后是否需要修改 true需要 false不需要 发送的pre无论匹配不匹配回复后都会true 心跳为false
	ConflictTerm  int  //在跟随者日志中与领导者发送的日志条目发生冲突的那条日志的任期号
	ConflictIndex int  //在跟随者日志中发生冲突的具体条目的索引。索引是日志条目在日志文件中的索引
}
//
// A Go object implementing a single Raft peer.
//
const (
    Leader = iota
    Follower
    Candidate
)
const (
    chanLen  = 10
    novote = -1
    electtimemax = 250
    electtimemin = 150
    heartsbeatmax = 400
    heartsbeatmin = 250
    HeartBeatInterval = 150
)
type Logentries struct{
    Command     interface{}
    Term        int
    Index       int
}
type Raft struct {
	mu     sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
   
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.



    //2A
    term              int                 // record time cricle
    state             int                 // three state Leader,Follower and Candidate
    //stateMutex        DebugRWMutex
    votecount         int
    votedfor          int
    appendEntriesChan chan struct{} // 心跳channel
    LeaderMsgChan     chan struct{}           // 当选Leader时发送
    VoteMsgChan       chan struct{}
    
    //2B
    CommitIndex       int
    LastApplied       int
    logs              []Logentries
    NextIndex         []int
    MatchIndex        []int
    applyCh           chan ApplyMsg


}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

    term = rf.term
    isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) EncoderState()[]byte {
    w:=new(bytes.Buffer)
    e := labgob.NewEncoder(w)
  
 
    if e.Encode(rf.term) != nil || e.Encode(rf.votedfor) != nil || e.Encode(rf.logs) != nil {
		log.Fatal("Errors occur when encode the data!")
	}
    data:=w.Bytes()
    return data
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

    rf.persister.SaveRaftState(rf.EncoderState())

}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var term int
    var votedfor int
    var logs []Logentries

    rf.mu.Lock()
	defer rf.mu.Unlock()
    if d.Decode(&term) != nil || d.Decode(&votedfor) != nil || d.Decode(&logs) != nil {
		log.Fatal("Errors occur when decode the data!")
    }else {
		// 解码成功后，将读取的状态信息赋值给Raft实例的对应字段
		rf.term = term
		rf.votedfor = votedfor
		rf.logs =  logs
        rf.LastApplied = rf.logs[0].Index
        rf.CommitIndex = rf.logs[0].Index
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	// 保存状态和日志

    rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persister.SaveStateAndSnapshot(rf.EncoderState(), snapshot)
    rf.trimIndex(index)
}
func (rf *Raft) trimIndex(index int) {
	snapShotIndex := rf.getFirstLog().Index
	if snapShotIndex >= index {
		return
	}
	// rf.logs[0]保留快照的lastLog，快照中包含的最后一条日志也会被保留下来，而不会被修剪掉
	// 释放大切片内存
	rf.logs = append([]Logentries{}, rf.logs[index-snapShotIndex:]...)
    rf.persist()
	rf.logs[0].Command = nil
	rf.debugprintlog("trim")
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term      int
    Candidate int
	LastLogIndex int // 候选人最后一个日志条目的索引(5.4节)
	LastLogTerm  int // 候选人最后一个日志条目的任期(5.4节)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Success    bool
    Term      int
}
var (
	debugMode  = true
	debugLogger *log.Logger
)

func init() {
	// 打开日志文件（追加模式）
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
	VOTE = 0
	LOG  = 1
	VOTEFLAG = true
	LOGFLAG  = true
)
func (rf *Raft)debugPrint(format string,flag int,args ...interface{}) {
	if debugMode && debugLogger != nil {
        term:=rf.term
        state:=rf.state
        stateStr := ""
        switch state {
        case Leader:
            stateStr = "Leader"
        case Follower:
            stateStr = "Follower"
        case Candidate:
            stateStr = "Candidate"
        }
  
		if ((flag == VOTE && VOTEFLAG) || (flag == LOG && LOGFLAG)){
			debugLogger.Printf("id:%v term:%v state:%v "+format+"\n",append([]interface{}{rf.me, term,stateStr}, args...)...)
		}
	}
}




//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
    defer rf.mu.Unlock()

	if args.Term <= rf.term{
		reply.Term,reply.Success=rf.term,false
        rf.debugPrint("request vote -- args.term little from %v",VOTE,args.Candidate)
		return
	}
	lastLog:=rf.getLastLog()
	if	(lastLog.Term > args.LastLogTerm) || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index){
		reply.Term,reply.Success=rf.term,false
		if args.Term >rf.term{
			rf.term = args.Term
            rf.persist()
		}
        rf.debugPrint("request vote -- args.term little from %v",VOTE,args.Candidate)
		return
	}


	if rf.votedfor == novote{
		rf.term =args.Term
		rf.votedfor = args.Candidate
        rf.persist()
        rf.state = Follower
		reply.Success = true
		reply.Term = rf.term
		rf.VoteMsgChan <- struct{}{}
		rf.debugPrint("vote success from %v",VOTE,args.Candidate)
	}else{
        if rf.votedfor == args.Candidate{
            rf.term =args.Term
            rf.votedfor = args.Candidate
            rf.persist()
            rf.state = Follower
            reply.Success = true
            reply.Term = rf.term
            rf.VoteMsgChan <- struct{}{}
        }else if !(rf.term == args.Term && args.LastLogTerm == lastLog.Term && args.LastLogIndex == lastLog.Index){
			rf.debugPrint("has voted but convert %v %v",VOTE,args.Candidate,rf.votedfor)
            rf.term =args.Term
			rf.votedfor = novote
            rf.persist()
			reply.Success = false
			reply.Term = args.Term
			rf.state = Follower
			rf.VoteMsgChan <- struct{}{}
			
		}else{
			reply.Success = false
			reply.Term = rf.term
			if rf.me > args.Candidate {
				rf.state = Follower
                rf.debugPrint("has voted but id %v %v",VOTE,args.Candidate,rf.votedfor)
				rf.votedfor = novote
                rf.persist()
				rf.VoteMsgChan<-struct{}{}
			}else{
                rf.debugPrint("has voted %v %v",VOTE,args.Candidate,rf.votedfor)
            }
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) ConvertToFollower(term int){
    rf.state = Follower
    rf.term = term
    rf.votedfor =novote
	rf.persist()
}
func (rf *Raft) getLastLog()Logentries{
    return rf.logs[len(rf.logs)-1]
}

func (rf *Raft)ConvertToLeader(){
    rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
    for i := range rf.NextIndex {
		rf.NextIndex[i] = rf.getLastLog().Index + 1
	}
    rf.state = Leader
    rf.persist()
}
func (rf *Raft)ConvertToCandidate(){
    rf.term++
    rf.state = Candidate
    rf.votecount = 1
    rf.votedfor = rf.me
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

    rf.mu.Lock()
	defer rf.mu.Unlock()


	if !ok {
        return false
    }

  
    //reply的term更大，转follower
    if reply.Term > rf.term {
		rf.debugPrint("vote receive -- reply.term bigger",VOTE)
        rf.ConvertToFollower(reply.Term)
        rf.VoteMsgChan <- struct{}{}
        return true
    }

   

	if rf.state != Candidate || args.Term != rf.term {
		rf.debugPrint("vote receive -- exit candidate or term add",VOTE)
        return true
    }
	
    if reply.Success  {
        rf.votecount++
        rf.debugPrint("get %v votes",VOTE,rf.votecount)
      
        // 如果获得超过半数的票数，并且仍是候选者，转换为领导者
     
        if 2*rf.votecount > len(rf.peers)&& rf.state == Candidate{
           rf.ConvertToLeader()
           // 超半数票 直接当选，当选为领导者后，通知 LeaderMsgChan
           rf.LeaderMsgChan <- struct{}{}
		   rf.debugPrint("get enough vote become leader",VOTE)
        }
     }
	 
     return true
  }



func (rf *Raft) sendAllRequestVote(){
    
	rf.mu.Lock()
	defer rf.mu.Unlock()

    tempLastlog:=rf.getLastLog()
    args:=&RequestVoteArgs{
        Term: rf.term,
        Candidate: rf.me,
        LastLogIndex: tempLastlog.Index,
        LastLogTerm: tempLastlog.Term,
    }
    for i:=range rf.peers{ 
        if i!=rf.me && rf.state == Candidate {
            go func(id int){
                ret:=&RequestVoteReply{}
				
                rf.sendRequestVote(id,args,ret)           
            }(i)
        }
    }

}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft)appendLog(command interface{})Logentries{

    newlog:=Logentries{
        Command: command,
        Term: rf.term,
        Index:rf.logs[len(rf.logs)-1].Index + 1,//2C会因为多线程导致多个日志获得同一个索引，所以不能读索引一个锁添加一个锁
    }
    rf.logs = append(rf.logs, newlog)
    rf.persist()
    return newlog
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    
	rf.mu.Lock()
	defer rf.mu.Unlock()


	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
  

    state:=rf.state
    
    if state != Leader{
        return  -1,-1,false
    }
    
    newLog := rf.appendLog(command)
    index = newLog.Index
    term = newLog.Term
    rf.debugprintlog("start")
	return index, term, isLeader
}
func (rf *Raft) getLogs(index int) Logentries{
        firstlogindex := rf.logs[0].Index
		if index-firstlogindex > len(rf.logs)-1{
			return Logentries{
				Command: nil,
				Index: -1,
				Term: -1,
			}
		}else{
        	return rf.logs[index-firstlogindex]
		}
	}
func (rf *Raft)SendInstallSnapshotRpc(id int,args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	ok := rf.peers[id].Call("Raft.InstallSnapshotHandler", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()



	if !ok {
		return
	}

    if args.Term != rf.term {
		// 领导者的任期不匹配，忽略该响应
		return
	}

    if reply.Term > rf.term {
		// 跟随者任期大于领导者任期，领导者转换为追随者
		rf.ConvertToFollower(reply.Term)
	}

    snapShotIndex := rf.getFirstLog().Index
 
    if rf.term != args.Term || rf.state != Leader || args.LastIncludedIndex != snapShotIndex {
		return
	}

    rf.NextIndex[id] = max(rf.NextIndex[id], args.LastIncludedIndex+1)
	rf.MatchIndex[id] = max(rf.MatchIndex[id], args.LastIncludedIndex)
    rf.persister.SaveStateAndSnapshot(rf.EncoderState(), args.Data)
}
func (rf *Raft) SendAppendEntries(id int, args *AppendEntriesArgs, reply *AppendEntriesReply,templastId int) {
    // 调用指定节点的 AppendEntriesHandler 方法，并传递请求和响应结构
    
	
	ok :=rf.peers[id].Call("Raft.AppendEntriesHandler", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()


	
    if !ok {
        return 
    }
	
    // 如果当前节点不再是领导者，则直接返回

    if rf.state != Leader {
        return    
    }
 
    // 如果响应中的任期大于当前任期，当前节点会转换为跟随者
 
    if reply.Term > rf.term {
		rf.debugPrint("reply term more big",VOTE)
        rf.ConvertToFollower(args.Term)
        rf.LeaderMsgChan <- struct{}{}
        return
    }else{
        if reply.Success{
			if reply.ConflictIndex == 0{
				rf.NextIndex[id] = 0
				rf.debugPrint("need snapshot",LOG)
			}else if reply.ConflictIndex ==-1{
				rf.MatchIndex[id] = max(templastId,rf.MatchIndex[id])
                rf.NextIndex[id] = max(rf.MatchIndex[id]+1,rf.NextIndex[id])
				rf.checkCommitIndex()
				rf.debugPrint("logs append success",LOG)
			}else if reply.ConflictTerm == -1{
				rf.NextIndex[id] = reply.ConflictIndex+1
				rf.debugPrint(">lastlog",LOG)
			}else{
				lastIndex := -1
				for i := len(rf.logs) - 1; i >= 0; i-- {
					if rf.logs[i].Term == reply.ConflictTerm {
						lastIndex = i
						break
					}
				}
				if lastIndex != -1 {
					rf.NextIndex[id] = lastIndex + 1
				}else {
					rf.NextIndex[id] = reply.ConflictIndex
				}
				rf.debugPrint("deal Conflict",LOG)
			}
        }else{
			rf.debugPrint("deal nothing",LOG)
		}
		
    } 
}
/* func (rf * Raft)findnextid(ConflictIndex int,Conflic){
    rf.logsMutex.RLock()
    defer rf.logsMutex.RUnlock()

} */

func (rf *Raft) SendAllAppendEntries(){

	rf.mu.Lock()
	defer rf.mu.Unlock()


    for server := range rf.peers {
       // 对于每个不是当前节点的节点，leader 启动一个新的 goroutine 来发送 AppendEntries 请求
      
 
        if server != rf.me && rf.state == Leader {
          
            nextId:=rf.NextIndex[server]
            firstlog:=rf.getFirstLog()
            if nextId > firstlog.Index{
                nextId=nextId-firstlog.Index
                prelog:=rf.logs[nextId-1]
			
                logs := make([]Logentries, len(rf.logs)-nextId)
                copy(logs, rf.logs[nextId:]) // 拷贝尚未同步的日志
		
                templastId :=  rf.getLastLog().Index
                go func(id int) {
             
                    args := &AppendEntriesArgs{
                        Term:          rf.term,
                        LeaderId:      rf.me,
                        PrevLogIndex:  prelog.Index,
                        PrevLogTerm:   prelog.Term,
                        Entries:       logs,
                        LeaderCommit:  rf.CommitIndex, 
                    }
               
                    reply := &AppendEntriesReply{
                        Term:    0,
                        Success: false,
                        ConflictTerm: 0,
                        ConflictIndex: 0,
                    }
                    rf.SendAppendEntries(id, args, reply,templastId)
                }(server)    
            }else{
                rf.debugPrint("snapshot",LOG)
            
                args := &InstallSnapshotArgs{
					Term:              rf.term,
					LeaderId:          rf.me,
					LastIncludedIndex: firstlog.Index, // 快照保存的最后一条日志的索引
					LastIncludedTerm:  firstlog.Term,  // 快照保存的最后一条日志的任期
					Data:              rf.persister.ReadSnapshot(),
				}
                
                go func(id int, args *InstallSnapshotArgs) {
					reply := &InstallSnapshotReply{}
					rf.SendInstallSnapshotRpc(id, args, reply)
				}(server, args)

            }
        }
    }
}
func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	

	rf.mu.Lock()
	defer rf.mu.Unlock()




	term := rf.term

    reply.Term = term
	// 如果当前节点的任期大于请求的任期，则忽略请求
	if term > args.Term {
		return
	}

	if term < args.Term || (term == args.Term && rf.state == Candidate) {
		// 如果当前节点的任期小于请求的任期，或者任期相等（收到了leader的请求）则转换为追随者
        rf.ConvertToFollower(args.Term)
	}


	// 发送AppendEntriesReply，确认快照接收成功
	rf.appendEntriesChan <- struct{}{}
	// 如果请求中的快照最后包含的索引小于等于当前节点的已提交索引，则无需处理

	if args.LastIncludedIndex <= rf.CommitIndex {
		return
	}
	// 根据快照信息更新日志
   
	rf.CommitIndex = args.LastIncludedIndex
	rf.LastApplied = args.LastIncludedIndex
	rf.debugprintlog("install shot")
	if rf.getLastLog().Index <= args.LastIncludedIndex {
		// 如果所有旧的日志都已被快照覆盖，创建新的日志条目
		rf.logs = []Logentries{{
			Command: nil,
			Term:    args.LastIncludedTerm,
			Index:   args.LastIncludedIndex,
		}}
	} else {
		// 否则，保留快照之后的日志条目，并更新第一个日志条目的信息
		snapIndex := rf.getFirstLog().Index
		newLogs := make([]Logentries, rf.getLastLog().Index-args.LastIncludedIndex+1)
		copy(newLogs, rf.logs[args.LastIncludedIndex-snapIndex:])
		rf.logs = newLogs
		rf.logs[0].Command = nil
		rf.logs[0].Term = args.LastIncludedTerm
		rf.logs[0].Index = args.LastIncludedIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.EncoderState(), args.Data)
	// 异步发送ApplyMsg，通知应用层处理快照
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}()
}
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    // 传一个空结构体表示接收到了Leader的请求。
    // 初始化响应的任期为当前任期
 
	rf.mu.Lock()
	defer rf.mu.Unlock()


    if(rf.term > args.Term){
        reply.Term = rf.term
        return
    }else{
        //主要是为了一个leader断连后，其他集群选出新leader，旧leader快速转换成follower
		reply.Term = args.Term
        if rf.term < args.Term && rf.state!=Follower{
			rf.debugPrint("hearts contorl follower",VOTE)
            rf.state = Follower
            rf.term = args.Term
            rf.votedfor =novote
            rf.persist()
			rf.appendEntriesChan <- struct{}{}
        }else{   
            rf.term = args.Term
            rf.persist()        }
        lastlog:=rf.getLastLog()    
		firstlog:=rf.getFirstLog()
		if args.PrevLogIndex > lastlog.Index{
			reply.Success = true
			reply.ConflictIndex = lastlog.Index
			reply.ConflictTerm = -1
			rf.debugPrint("1-------- from %v",LOG,args.LeaderId)
		}else if args.PrevLogIndex < firstlog.Index{
			reply.Success = true
			reply.ConflictIndex	= 0
			reply.ConflictTerm = 0
			rf.debugPrint("2-------- ",LOG)
		}else{
			onelog := rf.getLogs(args.PrevLogIndex)
			if onelog.Term == args.PrevLogTerm{
				reply.Success = true
				reply.ConflictIndex = -1
				reply.ConflictTerm = -1
				if len(args.Entries) == 0{
					rf.debugPrint("get hearts from %v",LOG,args.LeaderId)
				}else{
                    
					for idx := 0; idx < len(args.Entries); idx++ {
                        curIdx := idx + args.PrevLogIndex + 1 - firstlog.Index
                        if curIdx >= len(rf.logs) || rf.logs[curIdx].Term != args.Entries[idx].Term {
                            rf.logs = append(rf.logs[:curIdx], args.Entries[idx:]...)
                            break
                        }
                    }
                    rf.persist()
                    rf.debugprintlog("append success from",args.LeaderId)
				}
                nowlastlog := rf.getLastLog()
                if args.LeaderCommit > rf.CommitIndex {
                    rf.CommitIndex = min(nowlastlog.Index, args.LeaderCommit)
                }
			}else{
				var i int
				for i = onelog.Index-1; i>=firstlog.Index && rf.logs[i-firstlog.Index].Term==onelog.Term ;i--{}
				if i < firstlog.Index{
					reply.Success = true
					reply.ConflictIndex = 0
					reply.ConflictTerm = 0
				}else{
					reply.Success = true
					answerlog:=rf.getLogs(i+1)
					reply.ConflictIndex = answerlog.Index
					reply.ConflictTerm = answerlog.Term
				}
				rf.debugPrint("return conflict",LOG)
			}
		}
        rf.appendEntriesChan <- struct{}{}
    }
}
//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft)checkCommitIndex(){
 


    templastId :=  rf.getLastLog().Index
    for i := rf.CommitIndex + 1; i <= templastId; i++ {
        count := 1 
        for j := range rf.peers {
            if rf.MatchIndex[j] >= i && j != rf.me{
                count++
            }
        }
        if 2*count > len(rf.peers) { // 超过半数节点存储该日志
            rf.CommitIndex = i
        }
    }
}
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
        switch rf.state{
        case Candidate:
            go rf.sendAllRequestVote()
            select {
                case <-rf.VoteMsgChan:
                    continue
                case <-rf.appendEntriesChan:
                    continue
                case <- rf.LeaderMsgChan:
                    continue
                case <-time.After( time.Duration(rand.Intn(electtimemax-electtimemin) + electtimemin) * time.Millisecond):
                    // 选举超时 重置选举状态\
					if rf.state == Candidate{
                        rf.mu.Lock()
						rf.debugPrint("elect pass",VOTE)
                    	rf.ConvertToCandidate()
                        rf.persist()
                        rf.mu.Unlock()
					}
                    continue
            }
        case Follower:
            select {
                case <- rf.VoteMsgChan:
                    continue
                case  <-rf.appendEntriesChan:
                    continue
                case <-time.After( time.Duration(rand.Intn(heartsbeatmax-heartsbeatmin) + heartsbeatmin) * time.Millisecond):
                    // 附加日志条目超时，转换为候选人，发起选举
                    // 增加扰动避免多个Candidate同时进入选举
					rf.debugPrint("hearts pass",VOTE)
                    rf.mu.Lock()
                    rf.ConvertToCandidate()
                    rf.persist()
                    rf.mu.Unlock()              }
        case Leader: 
            rf.SendAllAppendEntries()
            select {
                case <- rf.appendEntriesChan:
                    continue
                case <-time.After(HeartBeatInterval*time.Millisecond):
                    continue
            }
        }

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func (rf *Raft)getFirstLog() Logentries{
    return rf.logs[0]
}
func (rf *Raft) doApplyWork() {
	for !rf.killed() {
		rf.applyLog() // 确保已提交的日志条目被及时应用到状态机上
		time.Sleep(50 * time.Millisecond)
	}
}
func (rf *Raft) applyLog() {


	if rf.CommitIndex <= rf.LastApplied {
		return
	}
    rf.mu.Lock()
    snapShotIndex := rf.getFirstLog().Index
	copyLogs := make([]Logentries, rf.CommitIndex-rf.LastApplied)
	copy(copyLogs, rf.logs[rf.LastApplied-snapShotIndex+1:rf.CommitIndex-snapShotIndex+1])
    rf.mu.Unlock()
    rf.debugPrint("apply logs %v",LOG,copyLogs)
	// 这里不要加锁 2D测试函数会死锁
	// 遍历从lastApplied+1到commitIndex的所有日志条目
	for _, logEntity := range copyLogs {
		// 将每个条目的命令通过applyChan发送出去，以供状态机执行
		rf.applyCh <- ApplyMsg{
			CommandValid: true,              // 包含一个新提交的日志条目
			Command:      logEntity.Command, // 新提交的日志条目
			CommandIndex: logEntity.Index,   // 新提交日志条目的索引
		}
	}
    rf.LastApplied = rf.CommitIndex
}



func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
    rf.mu=sync.Mutex{}
    rf.me = me
    rf.dead = 0

    // Your initialization code here (2A, 2B, 2C).


    //2A
	
  	rf.peers = peers
	rf.persister = persister
    rf.state = Follower
    rf.appendEntriesChan = make(chan struct{},chanLen) // 用于心跳信号的通道
    rf.LeaderMsgChan = make(chan struct{}, chanLen)      // 用于领导者选举信号的通道
    rf.VoteMsgChan = make(chan struct{},chanLen)
    rf.votedfor = novote

    //2B

  
    rf.CommitIndex  = 0
    rf.LastApplied = 0
    rf.logs = []Logentries{{nil,0,0}}
    rf.NextIndex  = make([]int,len(peers))
    rf.MatchIndex = make([]int,len(peers))
    rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    //2D

	
	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.doApplyWork()
	return rf
}
func (rf *Raft)debugprintlog(abc string,args ...interface{}){

    rf.debugPrint(abc+" %v",LOG,append(append([]interface{}{},args...),rf.logs))
}
