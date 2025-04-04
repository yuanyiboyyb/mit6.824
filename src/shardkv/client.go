package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	leaderid	map[int]int
    clientid int64
    commandid int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	

	ck.config = shardctrler.Config{}
	ck.config.Groups = map[int][]string{}
	ck.config.Num = 0


	ck.clientid = nrand()
    ck.commandid = 0
	ck.leaderid = make(map[int]int)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.commandid++
	args := GetArgs{}
	args.Key = key
	args.Clientid=ck.clientid
	args.Commandid=ck.commandid
	for {
		args.Num = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for {
				si:= ck.leaderid[gid]
				srv:=ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err==Errold) {
					break 
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader{
					ck.leaderid[gid]=(ck.leaderid[gid]+1)%len(servers)
				}
				if ok && (reply.Err == ErrWait || reply.Err == Errnew){
					time.Sleep(3000 * time.Millisecond)
				}

			}
		}
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		for key:=range ck.config.Groups{
			if _, ok := ck.leaderid[key]; !ok {
				ck.leaderid[key]=0
			}
		}
	}
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	ck.commandid++
	args.Commandid=ck.commandid
	args.Clientid=ck.clientid


	for {
		args.Num = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for {
				si:= ck.leaderid[gid]
				srv:=ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrFail) {
					return 
				}
				if ok && (reply.Err == ErrWrongGroup || reply.Err==Errold) {
					break
				}
				// ... not ok, or ErrWrongLeader
				if !ok || reply.Err == ErrWrongLeader{
					ck.leaderid[gid]=(ck.leaderid[gid]+1)%len(servers)
				}
				if ok && (reply.Err == ErrWait || reply.Err == Errnew){
					time.Sleep(3000 * time.Millisecond)
				}
			}
		}
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		for key:=range ck.config.Groups{
			if _, ok := ck.leaderid[key]; !ok {
				ck.leaderid[key]=0
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
