package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type WorkerStatus int
const (
    Free    WorkerStatus = iota
    Busy
    Timeout
)

type Coordinator struct {
	// Your definitions here.
    mu sync.Mutex

    MapTaskFinished bool
    MapTaskRemain int // 还剩多少 map task 任务可以分配
    ReduceTaskFinished bool
    ReduceTaskRemain int // 还剩多少 reduce task 任务可以分配

    Workers int
    WS map[int] WorkerStatus // WorkerStatus 表示工人目前的状态，0-表示空闲，1-表示正在做任务，2-表示 coordinator 已经联系不到工人了

    // map task
    WorkerToMapTask map[int] string// worker i 正在做 文件 filename 的 map task
    IntermediateFiles []string
    RecordFiles map[string] bool // 用于记录哪些中间文件已经出现过了
    MapTask map[string] int // map task 需要完成的文件还有哪些, 2 表示已经完成, 1 表示还未完成, 0 表示还未分配
    MapTaskBaseFilename string

    // reduce task
    NReduce int
    WorkerToReduceTask map[int] int// worker i 正在做 第 j 个 reduce task
    ReduceTask map[int] int // reduce task 需要完成的任务还有哪些, 2 表示已经完成, 1 表示还未完成, 0 表示还未分配
    ReduceTaskBaseFilename string

    // crash
    Timer map[int] int
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

func (c *Coordinator) IsDone(args *AskStatusArgs, reply *AskStatusReply) error {
    // 由于 Done 是线程安全的，因此 IsDone 也是线程安全的
    reply.IsDone = c.Done()
	return nil
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if args.WorkerId == -1 {
        args.WorkerId = c.Workers
        c.Workers++
    }
    // TODO
    // 分配任务
    worker_id := args.WorkerId
    reply.WorkerId = worker_id
    reply.NReduce = c.NReduce
    reply.XReduce = -1

    if !c.MapTaskFinished {
        reply.IsMapTask = true
        for filename, val := range c.MapTask {
            if val == 0 {
                reply.Filename = filename
                reply.MapTaskBaseFilename = c.MapTaskBaseFilename
                c.MapTask[filename] = 1
                c.WorkerToMapTask[worker_id] = filename
                c.WS[worker_id] = Busy
                c.Timer[worker_id] = 0
                break
            }
        }
    } else if !c.ReduceTaskFinished {
        reply.IsReduceTask = true
        for xreduce, val := range c.ReduceTask {
            if val == 0 {
                reply.XReduce = xreduce
                reply.ReduceTaskBaseFilename = c.ReduceTaskBaseFilename
                reply.AllFiles = c.IntermediateFiles
                c.ReduceTask[xreduce] = 1
                c.WorkerToReduceTask[worker_id] = xreduce
                c.WS[worker_id] = Busy
                c.Timer[worker_id] = 0
                break
            }
        }
    }
	return nil
}

func (c *Coordinator) is_timeout(worker_id int) bool {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.WS[worker_id] == Timeout
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
    worker_id := args.WorkerId
    // 如果超时了则不理他
    if  c.is_timeout(worker_id) {
        reply.GoodJob = false
        c.WS[worker_id] = Free
        return nil
    }

    c.mu.Lock()
    defer c.mu.Unlock()

    if !c.MapTaskFinished {
        if c.WorkerToMapTask[worker_id] == args.MapTaskFilename && c.WS[worker_id] == Busy {
            reply.GoodJob = true
            c.WS[worker_id] = Free

            for _, intermediate_file := range args.IntermediateFile {
                // 如果中间文件没有出现过，那么就把他加入 IntermediateFiles 中，并把他记录下了，用于去重
                _, ok := c.RecordFiles[intermediate_file]
                if !ok {
                    c.IntermediateFiles = append(c.IntermediateFiles, intermediate_file)
                    c.RecordFiles[intermediate_file] = true
                }
            }
            c.MapTask[args.MapTaskFilename] = 2

            c.MapTaskRemain--
            if c.MapTaskRemain == 0 {
                c.MapTaskFinished = true
            }
            return nil
        }
    } else if !c.ReduceTaskFinished{
        if c.WorkerToReduceTask[worker_id] == args.XReduce && c.WS[worker_id] == Busy {
            reply.GoodJob = true
            c.WS[worker_id] = Free

            c.ReduceTask[args.XReduce] = 2

            c.ReduceTaskRemain--
            if c.ReduceTaskRemain == 0 {
                c.ReduceTaskFinished = true
            }
            return nil
        }
    } else {
        // 所有任务都已经完成了
        reply.GoodJob = false
    }
    // worker 向我汇报了，但他汇报的任务和我发布的不同或者他在 free 或 timeout 状态
    // 但他既然向我汇报了，那么他一定是 Free 的
    c.WS[worker_id] = Free

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
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
    c.mu.Lock()
    defer c.mu.Unlock()
    if c.MapTaskFinished && c.ReduceTaskFinished {
        ret = true
    }
    for worker_id, _ := range c.Timer {
        c.Timer[worker_id]++
        if c.Timer[worker_id] >= 10 && c.WS[worker_id] == Busy {
            c.WS[worker_id] = Timeout
            if !c.MapTaskFinished {
                map_task := c.WorkerToMapTask[worker_id]
                c.WorkerToMapTask[worker_id] = ""
                c.MapTask[map_task] = 0
            } else if !c.ReduceTaskFinished {
                reduce_task := c.WorkerToReduceTask[worker_id]
                c.WorkerToReduceTask[worker_id] = -1
                c.ReduceTask[reduce_task] = 0
            }
        }
    }

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

    c.MapTaskRemain = len(files)
    c.ReduceTaskRemain = nReduce
    c.NReduce = nReduce

    c.MapTask = make(map[string]int)
    c.ReduceTask = make(map[int]int)
    c.WS = make(map[int] WorkerStatus)
    c.WorkerToMapTask = make(map[int] string)
    c.IntermediateFiles = []string{}
    c.RecordFiles = make(map[string] bool)
    c.WorkerToReduceTask = make(map[int] int)
    c.Timer = make(map[int] int)

    c.MapTaskBaseFilename = "mr"
    c.ReduceTaskBaseFilename = "mr-out"

    for _, file := range files {
        c.MapTask[file] = 0
    }

    for idx := 0; idx < nReduce; idx++ {
        c.ReduceTask[idx] = 0
    }

	c.server()
	return &c
}
