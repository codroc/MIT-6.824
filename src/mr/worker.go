package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "strings"
import "sort"
import "encoding/json"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func is_map_task(task AskTaskReply) bool {
    return task.IsMapTask
}

func printTaskReply(reply AskTaskReply) {
    fmt.Printf("IsMapTask = %v\n IsReduceTask = %v\n Filename = %v\n MapTaskBaseFilename = %v\n WorkerId = %v\n NReduce = %v\n ReduceTaskBaseFilename = %v\n XReduce = %v\n", reply.IsMapTask, reply.IsReduceTask, reply.Filename, reply.MapTaskBaseFilename, reply.WorkerId, reply.NReduce, reply.ReduceTaskBaseFilename, reply.XReduce)
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
    // 如果 mr 任务还没结束
    var nreduce int
    worker_id := -1
    total_map := 0
    total_reduce := 0
    for !IsDone() {
        fmt.Printf("Has finished %d task.\n", total_map + total_reduce)
        // 向 coordinator 要任务
        task := AskTask(worker_id)
        worker_id = task.WorkerId
        nreduce = task.NReduce
        buckets := make([][]KeyValue, nreduce) // nreduce 个 kva
        if is_map_task(task) {
            filename := task.Filename
            if filename != "" {
                file, err := os.Open(filename)
                if err != nil {
                    log.Fatalf("Can not open file %v ai", filename)
                }
                content, err := ioutil.ReadAll(file)
                file.Close()
                if err != nil {
                    log.Fatalf("Can not read file %v", filename)
                }
                kva := mapf(filename, string(content))
                for _, item := range kva {
                    bucket_number := ihash(item.Key) % nreduce
                    buckets[bucket_number] = append(buckets[bucket_number], item);
                }
                // 对 buckets 中的 item 排序
                for _, bucket := range buckets {
                    sort.Sort(ByKey(bucket))
                }

                intermediate_files := []string{}
                basename := task.MapTaskBaseFilename + "-" + strconv.Itoa(worker_id)
                for index, bucket := range buckets {
                    // TODO
                    // 创建一个临时文件，把 bucket 中的内容写入临时文件中，并在完成任务后通过 ReportTask 向 coordinator 汇报该任务，
                    // 当收到 coordinator 的确认后再把临时文件转正
                    oname := basename + "-" + strconv.Itoa(index)
                    intermediate_files = append(intermediate_files, oname)
                    ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
                    enc := json.NewEncoder(ofile)
                    for _, item := range bucket {
                        err := enc.Encode(&item)
                        if err != nil {
                            log.Fatalf("json encoding error!\n")
                        }
                    }
                    ofile.Close()
                }
                reply := ReportTask(worker_id, task.Filename, intermediate_files, task.XReduce)
                if reply.GoodJob {
                    total_map++
                    // var task_or_tasks string
                    // if total_map > 1 {
                    //     task_or_tasks = "tasks"
                    // } else {
                    //     task_or_tasks = "task"
                    // }
                    // fmt.Printf("worker %v finish %v map %v.\n", worker_id, total_map, task_or_tasks)
                }
            } else {
                // 暂时没任务，其他 worker 正在做 map task
                time.Sleep(time.Second)
                printTaskReply(task)
            }
        } else {
            // reduce task
            if task.XReduce != -1 {
                fmt.Printf("going to do reduce task %v\n", task.XReduce)
                intermediate := []KeyValue{}
                for _, file := range task.AllFiles {
                    ss := strings.Split(file, "-")
                    sxreduce := ss[len(ss) - 1]
                    xreduce, _ := strconv.Atoi(sxreduce)
                    if xreduce == task.XReduce {
                        f, _ := os.Open(file)
                        dec := json.NewDecoder(f)
                        for {
                            var kv KeyValue
                            if err := dec.Decode(&kv); err != nil {
                                break
                            }
                            intermediate = append(intermediate, kv)
                        }
                        f.Close()
                    }
                }
                sort.Sort(ByKey(intermediate))

                // 输出到 ReduceTaskBaseFilename-X 去
                oname := task.ReduceTaskBaseFilename + "-" + strconv.Itoa(task.XReduce)
                ofile, err := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
                if err != nil {
                    log.Fatalf("Can not open %v\n", oname)
                }
                i := 0
                for i < len(intermediate) {
                    j := i + 1
                    values := []string{intermediate[i].Key}
                    for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
                        values = append(values, intermediate[j].Key)
                        j++
                    }
                    output := reducef(intermediate[i].Key, values)
                    fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
                    i = j
                }
                ofile.Close()
                reply := ReportTask(worker_id, task.Filename, nil, task.XReduce)
                if reply.GoodJob {
                    total_reduce++
                }
            } else {
                // 暂时没有 reduce task 可做，其他 worker 正在做
                time.Sleep(time.Second)
                printTaskReply(task)
            }
        }
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// coordinator 是否已经完成了所有任务
func IsDone() bool {
    args := AskStatusArgs{}
    reply := AskStatusReply{}
    connect := call("Coordinator.IsDone", &args, &reply)
    if !connect {
        // coordinator 已经退出了，因为所有任务都已经完成了
        fmt.Printf("Coordinator is down!\n")
        os.Exit(0)
    }
    return reply.IsDone
}

// 请 coordinator 给我一个任务
func AskTask(worker_id int) AskTaskReply {
    args := AskTaskArgs{}
    args.WorkerId = worker_id
    reply := AskTaskReply{}
    reply.XReduce = -1 // 表示没有 reduce task 可做
    connect := call("Coordinator.AskTask", &args, &reply)
    if !connect {
        // coordinator 已经退出了，因为所有任务都已经完成了
        os.Exit(0)
    }
    return reply
}

// 向 coordinator 汇报我完成的任务
func ReportTask(worker_id int, filename string, intermediate_files []string, xreduce int) ReportTaskReply {
    args := ReportTaskArgs{}
    args.WorkerId = worker_id
    args.MapTaskFilename = filename
    args.IntermediateFile = intermediate_files
    args.XReduce = xreduce

    reply := ReportTaskReply{}

    connect := call("Coordinator.ReportTask", &args, &reply)
    if !connect {
        // coordinator 已经退出了，因为所有任务都已经完成了
        os.Exit(0)
    }
    return reply
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
