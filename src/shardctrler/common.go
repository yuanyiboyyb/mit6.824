package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}



type Err string
const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOk		   = "ErrOk"
)
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Seq int64
	Id 	int64
}

type LeaveArgs struct {
	GIDs []int
	Seq int64
	Id 	int64
}

type MoveArgs struct {
	Shard int
	GID   int
	Seq int64
	Id 	int64
}

type QueryArgs struct {
	Num int // desired config number
	Seq int64
	Id 	int64
}

type MyReply struct {
	Err         Err
	Config      Config
}
