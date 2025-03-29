package shardkv

import (
	"fmt"
	"runtime"
	"sync"
)


func (kv *ShardKV)init(){
	kv.mu = sync.Mutex{} 
}

func (kv *ShardKV) Lock() {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if isLeader{
		fmt.Printf(" id:%v [Lock] called from: %s\n",kv.gid,getCaller())
	}
}

// 写解锁
func (kv *ShardKV) Unlock() {
	_, isLeader := kv.rf.GetState()
	if isLeader{
		fmt.Printf("id:%v [Unlock] called from: %s\n",kv.gid,getCaller())
	}
	kv.mu.Unlock()
}
func getCaller() string {
	_, file, line, ok := runtime.Caller(2) // 2 级调用栈，获取真正调用者的行号
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", file, line)
}