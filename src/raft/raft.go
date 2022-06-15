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
    "sort"
)

func Min(a int, b int) int {
    if a < b {
        return a
    } else {
        return b
    }
}

func FindMiddleNumber(match_index map[int] int) int {
    arr := []int{}
    for _, value := range(match_index) {
        arr = append(arr, value)
    }
    sort.Ints(arr)
    if len(arr) % 2 == 0 {
        return arr[len(arr) / 2 - 1]
    } else {
        return arr[len(arr) / 2]
    }
}


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
    Term int
    Index int
    Command interface{}
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

    // 2B start
    // 所有机器可变状态，仅仅保存在内存中
    CommitIndex int // index < CommitIndex 的所有日志记录都已经 committed
    LastApplied int // index < LastApplied 的所有日志记录都已经被写入状态机

    // Leader 的可变状态，仅仅保存在内存中
    NextIndex map[int] int // 下条要复制给 follower i 的日志索引 j
    MatchIndex map[int] int // 
    // 2B end

    // 我自己定义的数据结构
    LeaderId int // leader id
    Status ServerStatus
    ReceivedAppendEntries bool
    VoteCount int // 已经获得的票数
    // VoteTerm int // 投票的时候，那位候选人的任期
    // 2B:
    applyCh chan ApplyMsg
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
    LastLogTerm int
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
    Term int
    LeaderId int
    // 2B:
    PreLogIndex int
    PreLogTerm int
    Entries []LogRecord
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
    // 2B:
    Duplicate bool
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
    reply.Term = rf.CurrentTerm

    MDebug(dLog2, "S%d: Vote request from S%d. My term = %d, leader term = %d.\n", rf.me, args.CandidateId, rf.CurrentTerm, args.Term)
    if args.Term < rf.CurrentTerm {
    } else {
        record := rf.Log[len(rf.Log) - 1]
        flag := args.LastLogTerm > record.Term || (args.LastLogTerm == record.Term && args.LastLogIndex >= record.Index)
        if args.Term > rf.CurrentTerm && flag {
            rf.Status = Follower
            rf.LeaderId = -1
            MDebug(dLog, "S%d become a Follower, because args.Term > rf.CurrentTerm && flag.\n", rf.me)
            rf.VotedFor = -1
        }
        if rf.VotedFor == -1 && flag {
            rf.VotedFor = args.CandidateId
            // rf.VoteTerm = args.Term
            rf.ReceivedAppendEntries = true
            reply.VoteGranted = true

            MDebug(dVote, "S%d vote to candidate S%d in term %d.\n", rf.me, args.CandidateId, args.Term)
        }
        // args.Term >= rf.CurrentTerm
        rf.CurrentTerm = args.Term
    }
}

func (rf *Raft) notifyClient() {
    for rf.killed() == false {
        rf.mu.Lock()
        for rf.LastApplied < rf.CommitIndex {
            rf.LastApplied++
            msg := ApplyMsg{}
            msg.CommandValid = true
            msg.Command = rf.Log[rf.LastApplied].Command
            msg.CommandIndex = rf.Log[rf.LastApplied].Index
            MDebug(dTrace, "S%d is going to apply log(term = %d, index = %d) to state machine.\n", rf.me, rf.Log[rf.LastApplied].Term, rf.Log[rf.LastApplied].Index)
            rf.mu.Unlock()
            rf.applyCh <- msg
            rf.mu.Lock()
        }
        rf.mu.Unlock()
        time.Sleep(10 * time.Millisecond)
    }
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
        MDebug(dLog, "S%d become a Follower, because received a heart beat package.\n", rf.me)
        reply.Success = true

        // 2B start
        leader_prelogindex := args.PreLogIndex
        leader_prelogterm := args.PreLogTerm
        found := false // 找到了 index = prelogindex，term = prelogterm 的日志记录，则表明在 index 之前的所有日志记录都和 leader 一致
        need_delete := false
        delete_index := -1
        for index, entry := range(rf.Log) {
            if entry.Index == leader_prelogindex && entry.Term == leader_prelogterm {
                found = true
                break
            } else if entry.Index == leader_prelogindex {
                need_delete = true
                delete_index = index
                break
            }
        }
        if len(args.Entries) > 0{ // 如果是日志复制的 rpc
        MDebug(dLog2, "S%d received a log replicate rpc. found = %v, need_delete = %v, leader CommitIndex = %d, my CommitIndex = %d\n", rf.me, found, need_delete, args.LeaderCommit, rf.CommitIndex)
            if !found {
                reply.Success = false
            }
            if !found && !need_delete { // 这种情况是论文中图7 的 a,b,e 的情况
            } else if !found && need_delete { // 这种情况是论文中图7 的 f 的情况
                rf.Log = rf.Log[:delete_index]
            } else if found { // 这种情况是论文中图7 的 c,d 的情况
                record := args.Entries[0]
                if record.Index < len(rf.Log) && rf.Log[record.Index].Term == record.Term {
                    // 如果已经有这条日志记录了
                    reply.Success = false
                    reply.Duplicate = true
                } else {
                    rf.Log = rf.Log[:leader_prelogindex + 1]
                    rf.Log = append(rf.Log, args.Entries...) // 如果 leader 复制的日志本地没有，则直接追加存储
                }
            }
        }
        if found && (args.LeaderCommit > rf.CommitIndex) {
            rf.CommitIndex = Min(args.LeaderCommit, len(rf.Log) - 1) // fix me? len(rf.Log) or len(rf.Log) - 1 ?
            // TODO: 如果成功应用日志到状态机则通过 applyCh 通知客户端
            // go rf.notifyClient()
        }
        // 2B end
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
    args.LastLogTerm = rf.Log[len(rf.Log) - 1].Term
    args.LastLogIndex = rf.Log[len(rf.Log) - 1].Index

    reply := RequestVoteReply{}
    rf.mu.Unlock()

    ok := rf.sendRequestVote(i, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.Status == Candidate {
        if ok && reply.LeaderTerm == rf.CurrentTerm { // 这里要保证这次投票结果是这一轮的，而不是上一轮的
            if reply.VoteGranted {
                rf.VoteCount++
                if rf.VoteCount >= (len(rf.peers) / 2 + 1) {
                    // 拿到了超过半数的选票，那么就当选领导者
                    rf.Status = Leader
                    rf.LeaderId = rf.me
                    MDebug(dLeader, "S%d become a leader in term %d!\n", rf.me, rf.CurrentTerm)
                    // go rf.ticket()
                    // 2B: TODO: 初始化 NextIndex 和 MatchIndex...
                    last_index_plus1 := rf.Log[len(rf.Log) - 1].Index + 1
                    MDebug(dLog, "initial nextindex = %d\n", last_index_plus1)
                    for j, _ := range(rf.peers) {
                        rf.NextIndex[j] = last_index_plus1
                        rf.MatchIndex[j] = 0
                    }
                    rf.MatchIndex[rf.me] = rf.NextIndex[rf.me] - 1
                }
            } else {
                if reply.Term > rf.CurrentTerm {
                    // 说明我是不可能在这一轮选举中获选了，因为我的 Term 太低了
                    // func Debug(topic logTopic, format string, a ...interface{})
                    MDebug(dLog2, "S%d become a Follower, because CurrentTerm = %d, but follower Term = %d.\n", rf.me, rf.CurrentTerm, reply.Term)
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

func (rf *Raft) waitSomeTimeAndSendAgain(i int) {
    time.Sleep(10 * time.Millisecond)
    rf.asyncSendAppendEntries(i)
}

func (rf *Raft) asyncSendAppendEntries(i int) {
    // 发送心跳包
    rf.mu.Lock()
    args := AppendEntriesArgs{}
    args.Term = rf.CurrentTerm
    args.LeaderId = rf.me
    // 2B start
    args.Entries = []LogRecord{}
    if rf.NextIndex[i] > 0 && rf.NextIndex[i] < len(rf.Log) {
        MDebug(dCommit, "S%d send replicate log rpc to S%d. rf.Log.size = %d, rf.NextIndex[i] = %d.\n", rf.me, i, len(rf.Log), rf.NextIndex[i])
        entry := rf.Log[rf.NextIndex[i] - 1]
        args.PreLogIndex = entry.Index
        args.PreLogTerm = entry.Term
        args.Entries = append(args.Entries, rf.Log[rf.NextIndex[i]])
    } else {
        MDebug(dWarn, "S%d's nextindex = %d.\n", i, rf.NextIndex[i])
    }
    args.LeaderCommit = rf.CommitIndex
    // 2B end
    reply := AppendEntriesReply{}
    rf.mu.Unlock()

    ok := rf.sendAppendEntries(i, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.Status == Leader {
        if ok {
            if reply.Success {
                if len(args.Entries) > 0 { // 如果是 日志复制
                    // 2B:
                    MDebug(dTrace, "S%d received response of replicate log rpc from S%d.\n", rf.me, i)
                    rf.NextIndex[i]++
                    rf.MatchIndex[i] = rf.NextIndex[i] - 1
                    MDebug(dCommit, "Leader is S%d, rf.NextIndex[%d] = %d, len(rf.Log) = %d.\n", rf.me, i, rf.NextIndex[i], len(rf.Log))
                    if rf.NextIndex[i] < len(rf.Log) {
                        MDebug(dLog2, "S%d is leader, and replicate log to S%d.\n", rf.me, i)
                        go rf.asyncSendAppendEntries(i)
                    }
                    // TODO: 如果超过半数的 follower 的 matchindex > commitindex，则更新 commitindex
                    // 其实就是找 matchindex 数组中的中位数 N
                    N := FindMiddleNumber(rf.MatchIndex)
                    MDebug(dCommit, "Zhong shu is %d.\n", N)
                    if N > rf.CommitIndex && rf.Log[N].Term == rf.CurrentTerm {
                        rf.CommitIndex = N
                        // TODO: 如果成功应用日志到状态机则通过 applyCh 通知客户端
                        // go rf.notifyClient()
                    }
                } else {
                    // 单纯的心跳包
                }
            } else {
                if reply.Term > rf.CurrentTerm {
                    MDebug(dWarn, "S%d's term %d is > mine election term %d, S%d is goint to be a Follower!\n", i, reply.Term, rf.CurrentTerm ,rf.me)
                    rf.CurrentTerm = reply.Term
                    rf.Status = Follower
                    rf.LeaderId = -1
                } else if len(args.Entries) > 0 && !reply.Duplicate {
                    // 2B:
                    // 由日志不一致导致的 false
                    MDebug(dLog2, "S%d's log is inconsistency with leader S%d.\n", i, rf.me)
                    rf.NextIndex[i]--
                    go rf.asyncSendAppendEntries(i)
                } else {
                    // duplicate
                }
            }
        } else {
            MDebug(dWarn, "S%d's rpc to S%d timeout!\n", rf.me, i)
            // rpc 调用超时
            // 2B start
            // go rf.waitSomeTimeAndSendAgain(i)
            // 2B end
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
    MDebug(dClient, "S%d receive a log append request from client.\n", rf.me)
    rf.mu.Lock()
    defer rf.mu.Unlock()

    isLeader = (rf.LeaderId == rf.me)
    if !isLeader {
        return index, term, isLeader
    }
    entry := LogRecord{}
    entry.Index = len(rf.Log)
    entry.Term = rf.CurrentTerm
    entry.Command = command
    rf.Log = append(rf.Log, entry)
    rf.NextIndex[rf.me] = len(rf.Log)
    rf.MatchIndex[rf.me] = len(rf.Log) - 1

    // 异步地并发 AppendEntries RPC 请求来进行日志复制
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        go rf.asyncSendAppendEntries(i)
    }
    index = entry.Index
    term = entry.Term
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
    rf.LeaderId = -1
    rf.ReceivedAppendEntries = true
    // rf.VoteTerm = rf.CurrentTerm
    rf.VotedFor = rf.me
    rf.VoteCount = 1
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
            MDebug(dTimer, "S%d's HeartBeat Timer timeout! ReceivedAppendEntries = %v, VotedFor = %v\n", rf.me, rf.ReceivedAppendEntries, rf.VotedFor)
            if !rf.ReceivedAppendEntries || rf.VotedFor == -1 {
                MDebug(dLog, "S%d is going to take part in candidate!\n", rf.me)
                rf.toBeACandidate()
            } else {
                rf.ReceivedAppendEntries = false
                // rf.VotedFor = -1
            }
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) ticketElectionTimeout() {
    random_number := rand.Intn(400 - 100) + 100
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
        time.Sleep(130 * time.Millisecond) // 每 130ms 发一次心跳包
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
    // 2B start
    rf.NextIndex = make(map[int] int)
    for idx, _ := range(rf.peers) {
        rf.NextIndex[idx] = rf.Log[len(rf.Log) - 1].Index + 1
    }
    rf.MatchIndex = make(map[int] int)
    for idx, _ := range(rf.peers) {
        rf.MatchIndex[idx] = 0
    }
    rf.applyCh = applyCh
    // 2B end

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.sendHeartBeatPeriodically()
    go rf.notifyClient()

	return rf
}
