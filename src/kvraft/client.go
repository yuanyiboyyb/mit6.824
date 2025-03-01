package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    leaderid int
    clientid int64
    commandid int
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
	// You'll have to add code here.
    ck.leaderid = 0
    ck.commandid = 0
    ck.clientid = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    ck.commandid++
    args := GetArgs{Key: key, Clientid: ck.clientid,Commandid: ck.commandid}
    serverId:=ck.leaderid
    for{
        reply:=GetReply{}
        ok:=ck.servers[serverId].Call("KVServer.Get",&args,&reply)
        if ok {
            if reply.Err == ErrNoKey{
				//DPrintf("client no key\n")
                ck.leaderid = serverId
                return ""
            }else if reply.Err == OK{
                ck.leaderid = serverId
				//DPrintf("client get value %v\n",reply.Value)
                return reply.Value
            }else if reply.Err == ErrWrongLeader{
                serverId = (serverId+1)%len(ck.servers)
                continue
            }
        }
        serverId = (serverId+1)%len(ck.servers)
    }
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    ck.commandid++
    serverId:=ck.leaderid
    args := PutAppendArgs{Key: key,Value:value,Op:op,Clientid: ck.clientid,Commandid: ck.commandid}
    for{
        reply:=PutAppendReply{}
        ok:=ck.servers[serverId].Call("KVServer.PutAppend",&args,&reply)
        if ok {
            if reply.Err == OK{
                ck.leaderid = serverId
				//DPrintf("client putappend {%v} {%v} {%v}\n",key,value,op)
                return
            }else if reply.Err == ErrWrongLeader{
                serverId = (serverId+1)%len(ck.servers)
                continue
            }
        }
        serverId = (serverId+1)%len(ck.servers)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
