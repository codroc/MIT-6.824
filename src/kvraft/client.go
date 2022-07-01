package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    mu sync.Mutex
    LeaderKV int
    Number int64 // command sequence
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
    MDebug(dWarn, "Make Again!\n")
    ck.Number = nrand()
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
    args := GetArgs{key, -1}
    reply := GetReply{}

    ck.mu.Lock()
    ck.Number += 1
    args.Number = ck.Number
    i := ck.LeaderKV
    ck.mu.Unlock()

    MDebug(dQury, "Quiry Key = %v, Command Number = %d\n", key, args.Number)
    for {
        reply = GetReply{}
        ok := ck.servers[i % len(ck.servers)].Call("KVServer.Get", &args, &reply)
        if ok {
            if reply.Err != ErrWrongLeader {
                ck.mu.Lock()
                ck.LeaderKV = i % len(ck.servers)
                ck.mu.Unlock()
                break
            }
        } else {
            MDebug(dRPC, "Quiry Key = %v Timeout!\n", key)
        }
        i++
    }
    if reply.Err == ErrNoKey {
        MDebug(dQury, "Quiry Result Key = %v, Value = EMPTY, Command Number = %d\n", key, args.Number)
        return ""
    } else {
        MDebug(dQury, "Quiry Result Key = %v, Value = %v, Command Number = %d\n", key, reply.Value, args.Number)
        return reply.Value
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
    args := PutAppendArgs{key, value, op, -1}
    reply := PutAppendReply{}

    ck.mu.Lock()
    ck.Number += 1
    args.Number = ck.Number
    i := ck.LeaderKV
    ck.mu.Unlock()

    MDebug(dPutA, "%v Key = %v, Value = %v, Command Number = %d\n", op, key, value, args.Number)
    for {
        reply = PutAppendReply{}
        ok := ck.servers[i % len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
        if ok {
            if reply.Err != ErrWrongLeader {
                ck.mu.Lock()
                ck.LeaderKV = i % len(ck.servers)
                ck.mu.Unlock()
                break
            }
        } else {
            MDebug(dRPC, "%v Key = %v, Value = %v Timeout, Command Number = %d!\n", op, key, value, args.Number)
        }
        i++
    }
    MDebug(dPutA, "%v Key = %v, Value = %v, Command Number = %d, Done\n", op, key, value, args.Number)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
