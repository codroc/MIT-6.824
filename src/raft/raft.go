package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
//	"bytes"
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
    "math/rand"
    "time"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// my code:
type ServerStatus int

const (
    Leader ServerStatus = iota
    Candidate
    Follower
)

type LogRecord struct {
    Item int
    Index int
    Command string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    // my code:
    // 所有机器需要持久化的状态，需要落盘
    CurrentTerm int
    VotedFor int
    Log []LogRecord

    // 所有机器可变状态，仅仅保存在内存中
    // CommitIndex int // index < CommitIndex 的所有日志记录都已经 committed
    // LastApplied int // index < LastApplied 的所有日志记录都已经被写入状态机

    // // Leader 的可变状态，仅仅保存在内存中
    // NextIndex map[int] int // 下条发给 int 的日志索引
    // MatchIndex map[int] int // 

    // 我自己定义的数据结构
    LeaderId int // leader id
    Status ServerStatus
    ReceivedAppendEntries bool
    VoteNumber int // 已经获得的票数
    VoteTerm int // 投票的时候，那位候选人的任期
    RestartElectionTimer bool // 重启选举定时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    // my code:
    rf.mu.Lock()
    defer rf.mu.Unlock()

    term = rf.CurrentTerm
    if rf.LeaderId == rf.me {
        isleader = true
    } else {
        isleader = false
    }
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    // my code:
    Term int
    CandidateId int
    LastLogIndex int
    LastLogItem int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
    LeaderTerm int // 发起请求者的任期
}

type AppendEntriesArgs struct {
    LeaderId int
    Term int
}

type AppendEntriesReply struct {
    Term int
    Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    // my code:
    // TODO: follower 收到投票请求后会怎么做？
    rf.mu.Lock()
    defer rf.mu.Unlock()

    reply.VoteGranted = false
    reply.LeaderTerm = args.Term

    MDebug(dLog2, "S%d: Vote request from S%d.\n", rf.me, args.CandidateId)
    if args.Term < rf.CurrentTerm {
    } else {
        rf.CurrentTerm = args.Term

        record := rf.Log[len(rf.Log) - 1]
        flag := args.LastLogItem > record.Item || (args.LastLogItem == record.Item && args.LastLogIndex >= record.Index)
        if rf.VotedFor == -1 && flag {
            rf.VotedFor = args.CandidateId
            rf.VoteTerm = args.Term
            rf.Status = Follower
            reply.VoteGranted = true

            MDebug(dVote, "S%d vote to candidate S%d in term %d.\n", rf.me, args.CandidateId, args.Term)
        } else if rf.VotedFor != -1 {
            // 已经投过票了
            if args.Term > rf.VoteTerm {
                rf.VotedFor = args.CandidateId
                rf.VoteTerm = args.Term
                rf.Status = Follower
                reply.VoteGranted = true

                MDebug(dVote, "S%d vote agian! But to candidate S%d in term %d.\n", rf.me, args.CandidateId, args.Term)
            }
        }
    }
    reply.Term = rf.CurrentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.CurrentTerm {
        reply.Term = rf.CurrentTerm
        reply.Success = false
    } else {
        // 心跳包：
        MDebug(dTrace, "S%d received append package from S%d\n", rf.me, args.LeaderId)
        rf.CurrentTerm = args.Term
        rf.LeaderId = args.LeaderId
        rf.ReceivedAppendEntries = true
        rf.Status = Follower
        rf.VotedFor = -1
        reply.Term = rf.CurrentTerm
        reply.Success = true
    }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// my code:
// 线程安全
func (rf *Raft) asyncSendRequestVote(i int) {
    rf.mu.Lock()
    args := RequestVoteArgs{}
    args.Term = rf.CurrentTerm
    args.CandidateId = rf.me
    args.LastLogItem = rf.Log[len(rf.Log) - 1].Item
    args.LastLogIndex = rf.Log[len(rf.Log) - 1].Index

    reply := RequestVoteReply{}
    rf.mu.Unlock()

    ok := rf.sendRequestVote(i, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.Status == Candidate {
        if ok && reply.LeaderTerm == rf.CurrentTerm { // 这里要保证这次投票结果是这一轮的，而不是上一轮的
            if reply.VoteGranted {
                rf.VoteNumber++
                if rf.VoteNumber >= (len(rf.peers) / 2 + 1) {
                    // 拿到了超过半数的选票，那么就当选领导者
                    rf.Status = Leader
                    rf.LeaderId = rf.me
                    MDebug(dLeader, "S%d become a leader!\n", rf.me)
                    // go rf.ticket()
                }
            } else {
                if reply.Term > rf.CurrentTerm {
                    // 说明我是不可能在这一轮选举中获选了，因为我的 Term 太低了
                    // func Debug(topic logTopic, format string, a ...interface{})
                    MDebug(dLog2, "S%d CurrentTerm = %d, but follower Term = %d\n", rf.CurrentTerm, reply.Term)
                    rf.CurrentTerm = reply.Term
                    rf.Status = Follower
                } else {
                    // follower 已经投过票了，或者我发给了 竞争者，或者当前节点的日志不是最新的
                }
            }
        } else {
            // rpc 调用超时，或者投票结果是上一轮的，是过期的
        }
    } else if rf.Status == Follower {
        // 如果发现已经有新的领导者了
        // 如果发现 follower 发来的 term 比自己的都大
        // 
    } else {
        // 已经从 Candidate 变成领导者了
    }
}

func (rf *Raft) asyncSendAppendEntries(i int) {
    // 发送心跳包
    rf.mu.Lock()
    args := AppendEntriesArgs{}
    args.Term = rf.CurrentTerm
    args.LeaderId = rf.me
    reply := AppendEntriesReply{}
    rf.mu.Unlock()

    ok := rf.sendAppendEntries(i, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.Status == Leader {
        if ok {
            if reply.Success {
            } else {
                if reply.Term > rf.CurrentTerm {
                    rf.CurrentTerm = reply.Term
                    rf.Status = Follower
                }
            }
        } else {
            MDebug(dWarn, "rpc to S%d timeout!\n", i)
            // rpc 调用超时
        }
    } else if rf.Status == Follower {
    } else {
    }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// my code:
// 非线程安全，需要外部加锁保证线程安全
func (rf *Raft) toBeACandidate() {
    rf.Status = Candidate
    rf.CurrentTerm++
    rf.VoteTerm = rf.CurrentTerm
    rf.VotedFor = rf.me
    rf.VoteNumber = 1
    go rf.ticketElectionTimeout()
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        MDebug(dLog, "S%d send vote reqeust to S%d.\n", rf.me, i)
        go rf.asyncSendRequestVote(i)
    }
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
        // my code:
        // 根据 paper 一般随机睡眠 150ms~300ms
        random_number := rand.Intn(500 - 300) + 300
        time.Sleep(time.Duration(random_number) * time.Millisecond)

        rf.mu.Lock()

        if rf.Status == Follower {
            // 心跳超时定时器
            MDebug(dTimer, "S%d's HeartBeat Timer timeout!\n", rf.me)
            if !rf.ReceivedAppendEntries && rf.VotedFor == -1 {
                MDebug(dLog, "S%d is going to take part in candidate!\n", rf.me)
                rf.toBeACandidate()
            } else {
                rf.ReceivedAppendEntries = false
            }
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) ticketElectionTimeout() {
    random_number := rand.Intn(200 - 100) + 100
    time.Sleep(time.Duration(random_number) * time.Millisecond)

    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.Status == Candidate {
        MDebug(dLog, "S%d's Election Timer timeout!\n", rf.me)
        rf.toBeACandidate()
    } else if rf.Status == Follower {
    }
}

func (rf *Raft) sendHeartBeatPeriodically() {
    for rf.killed() == false {
        time.Sleep(130 * time.Millisecond) // 每 130ms 发一次心跳包

        rf.mu.Lock()
        if rf.Status == Leader {
            for i := 0; i < len(rf.peers); i++ {
                if i == rf.me {
                    continue
                }
                MDebug(dTimer, "S%d send HeartBeat package to S%d.\n", rf.me, i)
                go rf.asyncSendAppendEntries(i)
            }
        }
        rf.mu.Unlock()
    }
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
    // my code:
    rf.CurrentTerm = 0
    rf.VotedFor = -1
    rf.LeaderId = -1
    rf.Status = Follower
    rf.Log = []LogRecord{LogRecord{}}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.sendHeartBeatPeriodically()

	return rf
}
