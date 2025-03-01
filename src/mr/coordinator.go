package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	Status int
	MapChan chan *Task
	ReduceChan chan *Task
	TaskNum    int      
	ReducerNum int
    wokerId   int
}
var (
    idmutex      sync.Mutex
	mutex        sync.Mutex //获取任务时的锁
	taskMutex    sync.Mutex //修改下面taskOK和reduceTaskOK时的锁
	taskOK       map[int]struct{} // 用以存放map阶段任务是否完成
	reduceTaskOK map[int]struct{}// 用以存放reduce阶段任务是否完成
	inChanmutex  sync.Mutex
	inChan       map[int]int //存放任务被哪个Worker拿走，没被拿走即为-1
	mapFinish    int //map任务完成数
    mapFinishmutex sync.Mutex
	reduceFinish int //reduce任务完成数
    reduceFinishmutex sync.Mutex
)
const (
	MapStatus int = iota
	ReduceStatus
	Waiting
	MapWaiting
	ReduceWaiting
	Success
)
type TaskReq struct {
	WorkerId int
}

type Task struct {
	Type       int //系统状态类型
	ID         int //任务ID
	ReducerNum int //reducer数量
	FileName   string //文件名
}
type CheckReq struct {
	Task     int //任务ID
	Type     int //任务类型
	WorkerId int //worker id
}
type CheckResp struct{
	Success   bool
}
type WorkIdResp struct{
    WorkId    int
} 

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}




//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

    go func() {
        http.Serve(l, nil)
    }()
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	mapFinishmutex.Lock()
	defer mapFinishmutex.Unlock()
	reduceFinishmutex.Lock()
	defer reduceFinishmutex.Unlock()
    if mapFinish == c.TaskNum && reduceFinish == c.ReducerNum {
        ret = true
    }
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//

func (c *Coordinator)produceTask(files []string){
	inChanmutex.Lock()
	defer inChanmutex.Unlock()
	for i,file := range files{
		task:=Task{
			Type:         MapStatus,
			ID:           i,
			ReducerNum:   c.ReducerNum,
			FileName:     file,
		}
		inChan[i] = -1
		c.MapChan <- &task
	}
}
func (c *Coordinator) makeReduceWork() {
	inChanmutex.Lock()
	defer inChanmutex.Unlock()
	for i := 0; i < c.ReducerNum; i++ {
		task := &Task{
			Type:       ReduceStatus, ////channel里存了reduce任务，当前状态必然是reducestatus
			ID:         i,
			ReducerNum: c.ReducerNum,
			FileName:   "",
		}
		inChan[i] = -1
		c.ReduceChan <- task
	}
}
func (c *Coordinator) PullId(_ *struct{},reply *WorkIdResp) error{
    idmutex.Lock()
    defer idmutex.Unlock()
    reply.WorkId = c.wokerId
    c.wokerId++
    return nil
}
func (c *Coordinator) PullTask(req *TaskReq, resp *Task) error{
	mutex.Lock()
	defer mutex.Unlock()
	switch c.Status{
		case MapStatus:
			{
				if len(c.MapChan) > 0 {
					*resp = *<-c.MapChan
					inChan[resp.ID] = req.WorkerId
                    go func(temp Task) {
                        timer := time.NewTimer(10 * time.Second)
                        defer timer.Stop()
                        <-timer.C
                        {
                            taskMutex.Lock()
                            if _, ok := taskOK[temp.ID]; !ok {
                                c.MapChan <- &temp
                                c.Status = MapStatus
                                inChan[resp.ID] = -1
                                log.Printf("map time exceeded %v\n", temp.ID)
                            }
                            taskMutex.Unlock()
                        }
                    }(*resp)
				}else {
					c.Status = MapWaiting
					resp.Type = Waiting
				}
			}
		case ReduceStatus:
			{
				if len(c.ReduceChan) > 0 {
					*resp = *(<-c.ReduceChan)
					resp.Type = ReduceStatus
					temp := *resp
					inChanmutex.Lock()
					inChan[resp.ID] = req.WorkerId
					inChanmutex.Unlock()
					go func(temp Task) {
                        timer := time.NewTimer(10 * time.Second)
                        defer timer.Stop()
                        <-timer.C
                        {
                            taskMutex.Lock()
                            if _, ok := reduceTaskOK[temp.ID]; !ok {
                                c.ReduceChan <- &temp
                                c.Status = ReduceStatus
                                inChan[temp.ID] = -1
                                log.Printf("reduce time exceeded %v\n", temp.ID)
                            }
                            taskMutex.Unlock()
                        }
					}(temp)
				}else {
					c.Status = ReduceWaiting
					resp.Type = Waiting
				}
			}
		case MapWaiting:
			{
				if mapFinish == c.TaskNum {
					c.makeReduceWork()
					c.Status = ReduceStatus
					resp.Type = Waiting
				} else {
					resp.Type = Waiting
				}
			}
		case ReduceWaiting:
			{
				if reduceFinish == c.ReducerNum {
					c.Status = Success
					resp.Type = Success
                    idmutex.Lock()
                    c.wokerId--
                    idmutex.Unlock()
				} else {
					resp.Type = Waiting
				}
	
			}
		default:
			{
				resp.Type = Success
			}
		}
		return nil
}
func (c *Coordinator) SuccessCheck(req *CheckReq, resp *CheckResp) error {
	taskMutex.Lock()
	inChanmutex.Lock()
	defer inChanmutex.Unlock()
	if inChan[req.Task] != req.WorkerId {
		taskMutex.Unlock()
		return nil
	}
	if req.Type == MapStatus {
		taskOK[req.Task] = struct{}{}
        mapFinishmutex.Lock()
        defer mapFinishmutex.Unlock()
		mapFinish++
		//log.Printf("map %v success by %v\n", req.Task, req.WorkerId)
	} else if req.Type == ReduceStatus {
		reduceTaskOK[req.Task] = struct{}{}
        reduceFinishmutex.Lock()
        defer reduceFinishmutex.Unlock()
		reduceFinish++
		//log.Printf("reduce %v success by %v\n", req.Task, req.WorkerId)
	}
	resp.Success = true
	taskMutex.Unlock()
	return nil
}
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Status:     MapStatus,
		TaskNum:    len(files),
		ReducerNum: nReduce,
		MapChan:    make(chan *Task, len(files)), // 必须有缓冲，否则会有阻塞
		ReduceChan: make(chan *Task, nReduce),
        wokerId:    1,
	}
    taskOK = make(map[int]struct{})
	reduceTaskOK= make(map[int]struct{})
	inChan = make(map[int]int)
	c.produceTask(files)
	c.server()
	return &c
}

