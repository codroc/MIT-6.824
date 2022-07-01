package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
    "time"
    // "fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Operation string // "Put" "Get" "Append"
    Key string
    Value string
    Number int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    cv *sync.Cond
    lastApplied int // 应用到状态机的绝对索引
    Database map[string]string
    Executed map[int64]int
    Index2Command map[int]int64
}

func (kv *KVServer) notCommit(index int, number int64) bool {
    return kv.lastApplied < index || kv.Index2Command[index] != number
}

func (kv *KVServer) isLeader() bool {
    _, isLeader := kv.rf.GetState()
    return isLeader
}

func (kv *KVServer) timeout(start time.Time) bool {
    return time.Since(start) > 100 * time.Millisecond
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    if kv.killed() {
        return
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if kv.isLeader() {
        command := Op{"Get", args.Key, "", args.Number}
        index, term, isLeader := kv.rf.Start(command)
        if !isLeader {
            reply.Err = ErrWrongLeader
        } else {
            MDebug(dPutA, "[KV%d L: %v] Wait Get Key = %v, Command Number = %d, Index = %d, Term = %d to be commited!\n", kv.me, kv.isLeader(), args.Key, args.Number, index, term)
            start := time.Now()
            for kv.notCommit(index, args.Number) {
                kv.cv.Wait()
                if kv.timeout(start) {
                    MDebug(dTimer, "[KV%d L: %v] Command Number = %d, Timeout!\n", kv.me, kv.isLeader(), args.Number)
                    reply.Err = ErrWrongLeader
                    return
                }
            }
            reply.Err = OK
            reply.Value = kv.Database[args.Key]
        }
    } else {
        reply.Err = ErrWrongLeader
    }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    if kv.killed() {
        return
    }
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if kv.isLeader() {
        operation := ""
        if args.Op == "Put" {
            operation = "Put"
        } else if args.Op == "Append" {
            operation = "Append"
        }

        command := Op{operation, args.Key, args.Value, args.Number}
        index, term, isLeader := kv.rf.Start(command)
        if !isLeader {
            reply.Err = ErrWrongLeader
        } else {
            MDebug(dPutA, "[KV%d L: %v] Wait %v Key = %v, Value = %v, Command Number = %d, Index = %d, Term = %d to be commited!\n", kv.me, kv.isLeader(), operation, args.Key, args.Value, args.Number, index, term)
            start := time.Now()
            for kv.notCommit(index, args.Number) {
                kv.cv.Wait()
                if kv.timeout(start) {
                    MDebug(dTimer, "[KV%d L: %v] Command Number = %d, Timeout!\n", kv.me, kv.isLeader(), args.Number)
                    reply.Err = ErrWrongLeader
                    return
                }
            }
            reply.Err = OK
        }
    } else {
        reply.Err = ErrWrongLeader
    }
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
    // msg := raft.ApplyMsg{}
    // msg.CommandValid = true
    // msg.Command = Op{}
    // kv.applyCh <- msg
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
    kv.cv = sync.NewCond(&kv.mu)
    kv.Database = make(map[string]string)
    kv.Executed = make(map[int64]int)
    kv.Index2Command = make(map[int]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

    go kv.applyLog()
    go kv.wakeupPeriodically()
	return kv
}

func (kv *KVServer) neverExecuted(op Op) bool {
    if _, ok := kv.Executed[op.Number]; ok {
        MDebug(dExe, "[KV%d L: %v] Command Number = %d has executed before!\n", kv.me, kv.isLeader(), op.Number)
        return false
    } else {
        return true
    }
}

func (kv *KVServer) hasExecuted(op Op) {
    kv.Executed[op.Number] = 1
}

func (kv *KVServer) applyLog() {
    for !kv.killed() {
        msg := <-kv.applyCh
        if kv.killed() {
            break
        }
        kv.mu.Lock()
        // apply msg to state machine
        op := msg.Command.(Op)
        if kv.neverExecuted(op) {
            if op.Operation == "Get" {
            } else if op.Operation == "Put" {
                kv.Database[op.Key] = op.Value
                MDebug(dPut, "[KV%d L: %v] Put Key = %v, Value = %v, Command Number = %d success!\n", kv.me, kv.isLeader(), op.Key, op.Value, op.Number)
            } else {
                kv.Database[op.Key] += op.Value
                MDebug(dPut, "[KV%d L: %v] Append Key = %v, Value = %v, Command Number = %d success!\n", kv.me, kv.isLeader(), op.Key, op.Value, op.Number)
            }
            kv.hasExecuted(op)
        }
        kv.lastApplied++
        kv.Index2Command[kv.lastApplied] = op.Number
        kv.cv.Broadcast()
        kv.mu.Unlock()
    }
}

func (kv *KVServer) wakeupPeriodically() {
    for !kv.killed() {
        time.Sleep(10 * time.Millisecond)
        kv.cv.Broadcast()
    }
}
