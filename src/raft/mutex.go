package raft

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
)

type  DebugRWMutex struct{
	mu         sync.Mutex
	id   		int
}
func (d *DebugRWMutex)init(){
	d.mu = sync.Mutex{}
    d.id = rand.Intn(1000) // 
}

func (d *DebugRWMutex) Lock() {
	d.mu.Lock()
	fmt.Printf(" id:%v [Lock] called from: %s\n",d.id,getCaller())

}

// 写解锁
func (d *DebugRWMutex) Unlock() {
	fmt.Printf("id:%v [Unlock] called from: %s\n",d.id,getCaller())
	d.mu.Unlock()
	
	
}
func getCaller() string {
	_, file, line, ok := runtime.Caller(2) // 2 级调用栈，获取真正调用者的行号
	if !ok {
		return "unknown"
	}
	return fmt.Sprintf("%s:%d", file, line)
}