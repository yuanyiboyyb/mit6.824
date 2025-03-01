package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//


type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}
func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}
func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func mapSolve(workerId int,mapf func(string, string) []KeyValue, task Task){
    filename := task.FileName
    file,err := os.Open(filename)
    if err != nil{
        log.Printf("open file error, worker 72, filename %v, %v", filename, err)
		return
    }
    defer file.Close()
    content,err :=io.ReadAll(file)
    if err != nil{
        log.Printf("read file to content error, worker 77, %v", err)
		return
    }
    kvSlice := mapf(filename,string(content))
    reduceNum := task.ReducerNum
    HashKv := make([][]KeyValue,reduceNum)
    for _,kv := range kvSlice{
        HashKv[ihash(kv.Key)%reduceNum] = append(HashKv[ihash(kv.Key)%reduceNum],kv)
    }
    for i:=0;i<reduceNum;i++{
        tempFile,err :=os.CreateTemp("/home/yyb/6.824/src/tmp","temp"+strconv.Itoa(workerId)+"-"+strconv.Itoa(i))
        if err != nil{
            log.Printf("map creat file error, worker 89 %v", err)
			return
        }
        for _,kv := range HashKv[i]{
            fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
        }
        OldName := tempFile.Name()
        err = os.Rename(OldName,"/home/yyb/6.824/src/tmp/"+strconv.Itoa(task.ID)+"-"+strconv.Itoa(i))
        if err != nil {
            log.Printf("rename file error, worker 99 %v", err)
			tempFile.Close()
			return
        }
        tempFile.Close()
    }
    req := CheckReq{
        Task:     task.ID,
		Type:     task.Type,
		WorkerId: workerId,
    }
    CallCheck(&req)
}   
func reduceSolve(workerId int, reducef func(string, []string) string, task Task) {
	id := strconv.Itoa(task.ID)
	files, err := os.ReadDir("/home/yyb/6.824/src/tmp/")
	if err != nil {
		log.Printf("read dir error, worker 116 %v", err)
		return
	}
	kv := make([]KeyValue, 0)
	mp := make(map[string][]string)
	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), id) {
			continue
		}
		file, err := os.Open("/home/yyb/6.824/src/tmp/" + fileInfo.Name())
		if err != nil {
			log.Printf("file open error, worker 127 %v\n", err)
			return
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Printf("file read all error, worker 132  %v\n", err)
			return
		}
		strContent := string(content)
		strSlice := strings.Split(strContent, "\n")
		for _, row := range strSlice {
			kvSlice := strings.Split(row, " ")
			if len(kvSlice) == 2 {
				mp[kvSlice[0]] = append(mp[kvSlice[0]], kvSlice[1])
			}
		}
	}
	for key, value := range mp {
		kv = append(kv, KeyValue{
			Key:   key,
			Value: reducef(key, value),
		})
	}
	newFile, err := os.CreateTemp("/home/yyb/6.824/src/tmp",id+"-temp")
	if err != nil {
		log.Printf("creat file error, worker 157 %v\n", err)
		return
	}
	sort.Sort(ByKey(kv))
	for _, v := range kv {
		fmt.Fprintf(newFile, "%v %v\n", v.Key, v.Value)
	}
	oldName := newFile.Name()
	defer newFile.Close()
	err = os.Rename(oldName, "mr-out-"+id)
	if err != nil {
		log.Printf("rename error worker 163 , %v\n", err)
		return
	}
	CallCheck(&CheckReq{
		Task:     task.ID,
		Type:     task.Type,
		WorkerId: workerId,
	})
}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
        workerId:= callPullId()
        //fmt.Println("new worker",workerId)
        for  {
            task := callPullTask(workerId)
            //fmt.Printf("task %+v\n",task)
            switch task.Type{
                case Success:
                    return
                case Waiting:
                    time.Sleep(2 * time.Second)
                case MapStatus:
                    {
                        mapSolve(workerId,mapf,task)
                    } 
                case ReduceStatus:
                    {
                        reduceSolve(workerId,reducef,task)
                    }
                default:
                    return
            }
            time.Sleep(time.Second*5)
        }
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}
func callPullId() int {
    args:=struct{}{}
    reply:=WorkIdResp{}
    call("Coordinator.PullId", &args, &reply)
    //fmt.Printf("workid %v\n", reply.WorkId)
    return reply.WorkId
}
func callPullTask(workerId int) Task{
    args:=TaskReq{WorkerId:workerId}
    reply:=Task{}
    ok := call("Coordinator.PullTask", &args, &reply)
    if ok {
        //fmt.Printf("workid %v get task\n",workerId)
        return reply
    }else{
        //fmt.Printf("failed get task\n")
        return reply
    }
}
func CallCheck(req *CheckReq){
    reply:=struct{}{}
    call("Coordinator.SuccessCheck",&req,&reply)
}
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
    sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
