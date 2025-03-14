package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	// Your data here.
	seq      int64
	id 		 int64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.seq = 0
	ck.id = nrand()
	ck.leaderId = -1

	return ck
}
func(ck *Clerk)getseq()(Sendseq int64){
	Sendseq = ck.seq
	ck.seq++
	return
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num,Seq: ck.getseq(),Id:ck.id}
	// Your code here.
	args.Num = num
	var reply MyReply
	for {
		for _, srv := range ck.servers {
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok{
				if reply.Err == OK{
					return reply.Config
				}else if reply.Err == ErrNoKey{
					return Config{}
				}else{
					ck.leaderId = (ck.leaderId+1)%len(ck.servers)
				}
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers,Seq: ck.getseq(),Id: ck.id}
	// Your code here.
	var reply MyReply
	for {
		for _, srv := range ck.servers {
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok {
				if reply.Err == OK{
					return
				}else{
					ck.leaderId = (ck.leaderId+1)%len(ck.servers)
				}
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids,Seq: ck.getseq(),Id: ck.id}
	// Your code here.
	var reply MyReply
	for {
		// try each known server.
		for _, srv := range ck.servers {
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok {
				if reply.Err == OK{
					return
				}else{
					ck.leaderId = (ck.leaderId+1)%len(ck.servers)
				}
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard,GID: gid,Seq: ck.getseq(),Id: ck.id}
	// Your code here.
	var reply MyReply
	for {
		// try each known server.
		for _, srv := range ck.servers {
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok {
				if reply.Err == OK{
					return
				}else{
					ck.leaderId = (ck.leaderId+1)%len(ck.servers)
				}
			}
		}
	}
}
