package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// AskTask: 向 coordinator 请求任务
type AskTaskArgs struct {
    WorkerId int // 当前 worker 的 id，刚开始没分配时为 nil
}

type AskTaskReply struct {
    IsMapTask bool
    IsReduceTask bool

    // map task
    Filename string // 需要做 map 的文件名字
    MapTaskBaseFilename string // 把 intermediate key 放到 MapTaskBaseFilename-WokerId-X 文件中去
    WorkerId int // coordinator 分配给当前 worker 的 id，只要它还活着，除了他自己以外就没人会占用这个 id

    // reduce task
    NReduce int // 总共需要多少个 reduce
    ReduceTaskBaseFilename string // reduce 任务的 base filename
    XReduce int // woker 要处理第 X 个 reduce 任务，并把输出放到 ReduceTaskBaseFilename-X 中去
    AllFiles []string // 所有的中间文件名
}

// AskStatus: 询问 coordinator 当前的状态
type AskStatusArgs struct {
}

type AskStatusReply struct {
    IsDone bool
}

// ReportTask: worker 完成了一个任务，向 coordinator 汇报该任务完成情况
type ReportTaskArgs struct {
    WorkerId int

    // map task
    MapTaskFilename string
    IntermediateFile []string // map 任务产生的中间文件

    // reduce task
    XReduce int // worker 做的是第 XReduce 个 reduce task
}

type ReportTaskReply struct {
    GoodJob bool
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
