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
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
    "math/rand"
    "time"
    "sort"
    "os"
)

func Max(a int, b int) int {
    if a > b {
        return a
    } else {
        return b
    }
}
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
    CommitIndex int // index <= CommitIndex 的所有日志记录都已经 committed
    LastApplied int // index <= LastApplied 的所有日志记录都已经被写入状态机

    // Leader 的可变状态，仅仅保存在内存中
    NextIndex map[int] int // 下条要复制给 follower i 的日志索引 j
    MatchIndex map[int] int // 
    // 2B end

    // 我自己定义的数据结构
    LeaderId int // leader id
    Status ServerStatus
    ReceivedAppendEntries bool
    VoteCount int // 已经获得的票数
    // 2B:
    applyCh chan ApplyMsg
    CVNotifyClient *sync.Cond
    CVSyncCommitIndex *sync.Cond
    // 2D:
    LastIncludedIndex int
    LastIncludedTerm int
    Checkpoint []byte
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
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.CurrentTerm)
    e.Encode(rf.VotedFor)
    e.Encode(rf.Log)
    // 2D:
    e.Encode(rf.LastIncludedIndex)
    e.Encode(rf.LastIncludedTerm)
	data := w.Bytes()
    rf.persister.SaveStateAndSnapshot(data, rf.Checkpoint)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
        MDebug(dPersist, "#####Not readPersist\n")
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
    MDebug(dPersist, "#####readPersist\n")
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var votedFor int
    var log []LogRecord

    var lastIncludedIndex int
    var lastIncludedTerm int
    if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
    d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
        MDebug(dWarn, "Decode error!\n")
        os.Exit(1)
    } else {
        rf.CurrentTerm = currentTerm
        rf.VotedFor = votedFor
        rf.Log = log

        rf.LastIncludedIndex = lastIncludedIndex
        rf.LastIncludedTerm = lastIncludedTerm
    }
    rf.Checkpoint = rf.persister.ReadSnapshot()
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
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if index > rf.Log[0].Index {
        rf.Checkpoint = snapshot
        rf.LastIncludedIndex = index
        rf.LastIncludedTerm = rf.GetLogRecordAbs(index).Term
        // TODO: 清理日志记录
        rf.Log = rf.Log[index - rf.Log[0].Index:]
        if rf.Log[0].Index != rf.LastIncludedIndex ||
        rf.Log[0].Term != rf.LastIncludedTerm {
            MDebug(dError, "Snapshot error!\n")
            os.Exit(1)
        }
        rf.persist()
    }
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
    // 为 2C figure8 做优化
    FirstIndex int
}

// 调用者需要加锁
func (rf *Raft) toBeFollower(leaderId int) {
    rf.Status = Follower
    rf.LeaderId = leaderId
    rf.VotedFor = leaderId
}

// 调用者需要加锁
func (rf *Raft) GetLogRecordAbs(absoluteIndex int) LogRecord {
    return rf.Log[absoluteIndex - rf.Log[0].Index]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    // my code:
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
        // MDebug(dLog, "flag = %v, args.LastLogTerm = %d > record.Term = %d, args.LastLogIndex = %d > record.Index = %d\n", flag, args.LastLogTerm, record.Term, args.LastLogIndex, record.Index)
        // TODO: code refactor
        // 如果候选者任期 > 我的任期，那么我无条件变成参与者，并开始准备投票
        changed := false
        if args.Term > rf.CurrentTerm {
            rf.CurrentTerm = args.Term
            rf.toBeFollower(-1)
            changed = true
        }
        // 如果候选者的日志比我的更新，并且我还没投过票，那么我把票头给他
        if rf.VotedFor == -1 && flag {
            rf.VotedFor = args.CandidateId
            rf.ReceivedAppendEntries = true
            reply.VoteGranted = true
            changed = true

            MDebug(dVote, "S%d vote to candidate S%d in term %d.\n", rf.me, args.CandidateId, args.Term)
        }
        if changed {
            rf.persist()
        }
    }
}

func (rf *Raft) notifyClient() {
    for rf.killed() == false {
        rf.mu.Lock()
        for rf.LastApplied < rf.CommitIndex {
            rf.LastApplied++
            MDebug(dLog, "S%d's Log size = %d, rf.Log[0].Index = %d, going to apply log index = %d\n", rf.me, len(rf.Log), rf.Log[0].Index, rf.LastApplied)
            msg := ApplyMsg{}
            msg.CommandValid = true
            msg.Command = rf.GetLogRecordAbs(rf.LastApplied).Command
            msg.CommandIndex = rf.GetLogRecordAbs(rf.LastApplied).Index
            MDebug(dClient, "S%d is going to apply log(term = %d, index = %d) to state machine. Commit Index = %d\n", rf.me, rf.GetLogRecordAbs(rf.LastApplied).Term, rf.GetLogRecordAbs(rf.LastApplied).Index, rf.CommitIndex)
            rf.mu.Unlock()
            rf.applyCh <- msg
            rf.mu.Lock()
        }
        rf.mu.Unlock()
        // TODO: 使用条件变量，而不是 sleep
        time.Sleep(10 * time.Millisecond)
    }
}

////////////// 用于 AppendEntries，非线程安全，调用者需加锁
func (rf *Raft) isHeartBeatPackage(args *AppendEntriesArgs) bool {
    return len(args.Entries) == 0
}
func (rf *Raft) myLogIsConsensusWithLeader(args *AppendEntriesArgs) bool {
    leader_prelogindex := args.PreLogIndex
    leader_prelogterm := args.PreLogTerm
    if leader_prelogindex <= rf.Log[0].Index {
        return true
    }
    found := false // 找到了 index = prelogindex，term = prelogterm 的日志记录，则表明在 index 之前的所有日志记录都和 leader 一致
    for _, entry := range(rf.Log) {
        if entry.Index == leader_prelogindex && entry.Term == leader_prelogterm {
            found = true
            break
        } else if entry.Index == leader_prelogindex {
            break
        }
    }
    return found
}

func (rf *Raft) appendNewEntries(args *AppendEntriesArgs) bool {
    // Append any new entries not already in the log
    changed := false
    j := 0
    i := 0
    for i = args.PreLogIndex + 1; i <= rf.lastIndex() && j < len(args.Entries); i++ {
        if i > rf.Log[0].Index {
            myEntry := rf.GetLogRecordAbs(i)
            if myEntry.Index != args.Entries[j].Index {
                break
            } else if myEntry.Term != args.Entries[j].Term {
                break
            }
        }
        j++
    }
    if i == rf.lastIndex() + 1 || j == len(args.Entries) {
        if i == rf.lastIndex() + 1 {
            rf.Log = append(rf.Log, args.Entries[j:]...)
            changed = true
        } else {
        }
    } else {
        rf.Log = rf.Log[:i - rf.Log[0].Index]
        rf.Log = append(rf.Log, args.Entries[j:]...)
        changed = true
    }
    return changed
}
func (rf *Raft) findFirstIndexInThisTerm() int {
    lastEntry := rf.GetLogRecordAbs(rf.lastIndex())
    lastTerm := lastEntry.Term
    ret := rf.Log[0].Index
    for index, entry := range(rf.Log) {
        if entry.Term == lastTerm {
            ret += index
            break
        }
    }
    return Max(ret, rf.CommitIndex)
}
////////////// 用于 AppendEntries，非线程安全，调用者需加锁
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.CurrentTerm {
        reply.Term = rf.CurrentTerm
        reply.Success = false
    } else {
        changed := false
        if rf.CurrentTerm != args.Term || rf.VotedFor != args.LeaderId {
            changed = true
        }
        rf.CurrentTerm = args.Term
        rf.toBeFollower(args.LeaderId)
        rf.ReceivedAppendEntries = true
        reply.Term = rf.CurrentTerm
        reply.Success = rf.myLogIsConsensusWithLeader(args)
        if rf.isHeartBeatPackage(args) {
            MDebug(dTrace, "S%d received HeartBeat package from S%d\n", rf.me, args.LeaderId)
        } else {
            // log entry append
            if reply.Success {
                MDebug(dLog2, "S%d received a log replicate rpc, and is consistent with leader S%d. Leader CommitIndex = %d, my CommitIndex = %d, append entry: I = %d, T = %d\n", rf.me, args.LeaderId, args.LeaderCommit, rf.CommitIndex, args.Entries[0].Index, args.Entries[0].Term)
                changed = rf.appendNewEntries(args) || changed
                if args.LeaderCommit > rf.CommitIndex {
                    MDebug(dCommit, "S%d's commit index change from %d to %d. size of rf.Log = %d.\n", rf.me, rf.CommitIndex, Min(args.LeaderCommit, len(rf.Log) - 1), len(rf.Log))
                    rf.CommitIndex = Min(args.LeaderCommit, rf.lastIndex())
                    // TODO: condition varable: notify client
                }
            } else {
                // 存在不一致，不一致的情况有以下几种
                if rf.lastIndex() >= args.PreLogIndex {
                    rf.Log = rf.Log[:args.PreLogIndex - rf.Log[0].Index]
                    changed = true
                }
                reply.FirstIndex = rf.findFirstIndexInThisTerm()
            }
        }
        if changed {
            rf.persist()
        }
    }
}

type SyncCommitIndexArgs struct {
    Term int
    LeaderId int
    MatchIndex int
    LeaderCommit int
}

type SyncCommitIndexReply struct {
    Term int
    Success bool
}

// TODO: 把同步 Commit Index 整合到心跳包 AppendEntries 中，减少 rpc 调用的次数，再看测试结果该优化是否能够显著降低系统延迟
// 2B: 周期性地让所有的 Follower 去跟随 Leader 的 CommitIndex
func (rf *Raft) SyncCommitIndex(args *SyncCommitIndexArgs, reply *SyncCommitIndexReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if args.Term < rf.CurrentTerm {
        reply.Term = rf.CurrentTerm
        reply.Success = false
    } else {
        changed := false
        if args.Term > rf.CurrentTerm {
            rf.CurrentTerm = args.Term
            rf.toBeFollower(args.LeaderId)
            rf.ReceivedAppendEntries = true
            changed = true
        }
        if args.LeaderCommit > rf.CommitIndex {
            a := Min(args.LeaderCommit, args.MatchIndex)
            b := Max(rf.CommitIndex, a)
            MDebug(dCommit, "S%d's commit index change from %d to %d. size of rf.Log = %d. args.LC = %d, args.MI = %d, rf.CI = %d\n", rf.me, rf.CommitIndex, b, len(rf.Log), args.LeaderCommit, args.MatchIndex, rf.CommitIndex)
            rf.CommitIndex = b
            // TODO: Notify Client
        }
        if changed {
            rf.persist()
        }
    }
}

func (rf *Raft) asyncSyncCommitIndex(i int) {
    if rf.killed() {
        return
    }
    rf.mu.Lock()
    if rf.LeaderId != rf.me {
        rf.mu.Unlock()
        return
    }
    args := SyncCommitIndexArgs{}
    args.Term = rf.CurrentTerm
    args.LeaderId = rf.me
    args.MatchIndex = rf.MatchIndex[i]
    args.LeaderCommit = rf.CommitIndex
    rf.mu.Unlock()
    reply := SyncCommitIndexReply{}

    ok := rf.sendSyncCommitIndex(i, &args, &reply)
    if ok {
    } else {
    }
}

func (rf *Raft) sendSyncCommitIndex(server int, args *SyncCommitIndexArgs, reply *SyncCommitIndexReply) bool {
	ok := rf.peers[server].Call("Raft.SyncCommitIndex", args, reply)
    return ok
}

func (rf *Raft) sendSyncCommitIndexPeriodically() {
    for rf.killed() == false {
        rf.mu.Lock()
        if rf.Status == Leader {
            for i := 0; i < len(rf.peers); i++ {
                if i == rf.me {
                    continue
                }
                go rf.asyncSyncCommitIndex(i)
            }
        }
        rf.mu.Unlock()
        // TODO: 用 cv
        time.Sleep(60 * time.Millisecond) // 每 130ms 发一次
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
    if rf.Status != Candidate {
        rf.mu.Unlock()
        return
    }
    args := RequestVoteArgs{}
    args.Term = rf.CurrentTerm
    args.CandidateId = rf.me
    args.LastLogTerm = rf.GetLogRecordAbs(rf.lastIndex()).Term
    args.LastLogIndex = rf.GetLogRecordAbs(rf.lastIndex()).Index

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
                    rf.Status = Leader
                    rf.LeaderId = rf.me
                    MDebug(dLeader, "S%d become a leader in term %d!\n", rf.me, rf.CurrentTerm)
                    last_index_plus1 := rf.GetLogRecordAbs(rf.lastIndex()).Index + 1
                    // MDebug(dLog, "initial nextindex = %d\n", last_index_plus1)
                    for j, _ := range(rf.peers) {
                        rf.NextIndex[j] = last_index_plus1
                        rf.MatchIndex[j] = 0
                    }
                    rf.MatchIndex[rf.me] = rf.NextIndex[rf.me] - 1
                    go rf.sendHeartBeat()
                }
            } else {
                if reply.Term > rf.CurrentTerm {
                    MDebug(dLeader, "S%d become a Follower, because CurrentTerm = %d, but follower Term = %d.\n", rf.me, rf.CurrentTerm, reply.Term)
                    rf.CurrentTerm = reply.Term
                    oldVoted := rf.VotedFor
                    rf.toBeFollower(-1)
                    rf.VotedFor = oldVoted
                    rf.persist()
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

/////////////////////用于 asyncSendAppendEntries，非线程安全，需要调用者加锁
func (rf *Raft) lastIndex() int {
    return len(rf.Log) - 1 + rf.Log[0].Index
}

func (rf *Raft) packageEntries(i int, args *AppendEntriesArgs) {
    if rf.lastIndex() >= rf.NextIndex[i] {
        // MDebug(dLog, "rf.lastIndex() = %d, rf.NextIndex[%d] = %d\n", rf.lastIndex(), i, rf.NextIndex[i])
        MDebug(dCommit, "S%d send replicate log rpc to S%d. rf.Log.size = %d, rf.NextIndex[i] = %d. PreLogIndex = %d, PreLogTerm = %d.\n", rf.me, i, len(rf.Log), rf.NextIndex[i], args.PreLogIndex, args.PreLogTerm)
        args.Entries = append(args.Entries, rf.Log[rf.NextIndex[i] - rf.Log[0].Index:]...)
    }
}

func (rf *Raft) inLocalLog(index int) bool {
    return index >= rf.Log[0].Index && index <= rf.lastIndex()
}
/////////////////////用于 asyncSendAppendEntries，非线程安全，需要调用者加锁
func (rf *Raft) asyncSendAppendEntries(i int) {
    if rf.killed() {
        return
    }
    // 发送心跳包
    rf.mu.Lock()
    if rf.LeaderId != rf.me {
        rf.mu.Unlock()
        return
    }
    args := AppendEntriesArgs{}
    args.Term = rf.CurrentTerm
    args.LeaderId = rf.me
    args.Entries = []LogRecord{}
    args.LeaderCommit = rf.CommitIndex
    MDebug(dWarn, "##################################Leader is S%d, rf.NextIndex[%d] = %d, size of rf.Log = %d\n", rf.me, i, rf.NextIndex[i], len(rf.Log))
    // 这里由于拍快照会把日志截断，所以做一下判断
    if !rf.inLocalLog(rf.NextIndex[i] - 1) {
        go rf.asyncInstallSnapshot(i)
        rf.mu.Unlock()
        return
    }
    entry := rf.GetLogRecordAbs(rf.NextIndex[i] - 1)
    args.PreLogIndex = entry.Index
    args.PreLogTerm = entry.Term
    rf.packageEntries(i, &args)
    original_index := rf.NextIndex[i]
    reply := AppendEntriesReply{}
    rf.mu.Unlock()

    ok := rf.sendAppendEntries(i, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.Status == Leader {
        if ok {
            if reply.Success { // [0, PreLogIndex] 范围的日志记录，Leader 和 Follower 达到了一致
                if len(args.Entries) > 0 && rf.NextIndex[i] == original_index { // 如果是 日志复制
                    // 2B:
                    rf.NextIndex[i] = args.PreLogIndex + len(args.Entries) + 1
                    rf.MatchIndex[i] = args.PreLogIndex + len(args.Entries)
                    MDebug(dCommit, "S%d received response of replicate log rpc from S%d, rf.NextIndex[%d] = %d, len(rf.Log) = %d.\n", rf.me, i, i, rf.NextIndex[i], len(rf.Log))
                    if rf.lastIndex() >= rf.NextIndex[i] {
                        MDebug(dLog2, "S%d is leader, and ##Continuely## replicate log to S%d.\n", rf.me, i)
                        go rf.asyncSendAppendEntries(i)
                    }
                    N := FindMiddleNumber(rf.MatchIndex)
                    MDebug(dCommit, "Zhong shu is %d.\n", N)
                    if N > rf.CommitIndex && rf.GetLogRecordAbs(N).Term == rf.CurrentTerm {
                        MDebug(dCommit, "Change Leader CommitIndex from %d to %d.\n", rf.CommitIndex, N)
                        rf.CommitIndex = N
                        // TODO: 用 cv
                    }
                } else {
                    // 单纯的心跳包
                }
            } else { // 只有任期太旧或日志不一致才会导致 reply.Success = false
                if reply.Term > rf.CurrentTerm {
                    MDebug(dLeader, "S%d's term %d is > mine election term %d, S%d is goint to be a Follower!\n", i, reply.Term, rf.CurrentTerm ,rf.me)
                    rf.CurrentTerm = reply.Term
                    oldVoted := rf.VotedFor
                    rf.toBeFollower(-1)
                    rf.VotedFor = oldVoted
                    rf.persist()
                } else if rf.NextIndex[i] == original_index {
                    // 2B:
                    // 由日志不一致导致的 false
                    MDebug(dLog2, "S%d's log is inconsistency with leader S%d.\n", i, rf.me)
                    rf.NextIndex[i] = reply.FirstIndex + 1
                    go rf.asyncSendAppendEntries(i)
                }
            }
        } else {
            MDebug(dWarn, "S%d's rpc to S%d timeout!\n", rf.me, i)
        }
    } else if rf.Status == Follower {
    } else {
    }
}

type InstallSnapshotArgs struct {
    Term int
    LeaderId int
    LastIncludedIndex int
    LastIncludedTerm int
    Snapshot []byte
}

type InstallSnapshotReply struct {
    Term int
    Success bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    if args.Term < rf.CurrentTerm {
        reply.Term = rf.CurrentTerm
        reply.Success = false
    } else {
        if args.Term > rf.CurrentTerm {
            rf.CurrentTerm = args.Term
            rf.toBeFollower(args.LeaderId)
            rf.persist()
        }
        reply.Term = rf.CurrentTerm
        reply.Success = true
        if rf.CommitIndex < args.LastIncludedIndex { // 如果快照太旧，那么替换快照并清理日志记录
            if args.LastIncludedIndex > rf.lastIndex() {
                rf.Log = rf.Log[:1]
            } else {
                rf.Log = rf.Log[args.LastIncludedIndex - rf.Log[0].Index:]
            }
            rf.Log[0].Index = args.LastIncludedIndex
            rf.Log[0].Term = args.LastIncludedTerm
            rf.LastIncludedIndex = args.LastIncludedIndex
            rf.LastIncludedTerm = args.LastIncludedTerm
            rf.LastApplied = args.LastIncludedIndex
            rf.CommitIndex = args.LastIncludedIndex
            rf.Checkpoint = args.Snapshot
            rf.persist()
            msg := ApplyMsg{}
            msg.SnapshotValid = true
            msg.Snapshot = rf.Checkpoint
            msg.SnapshotTerm = rf.LastIncludedTerm
            msg.SnapshotIndex = rf.LastIncludedIndex
            rf.mu.Unlock()
            rf.applyCh <- msg
            rf.mu.Lock()
        }
    }
    rf.mu.Unlock()
}

func (rf *Raft) asyncInstallSnapshot(i int) {
    if rf.killed() {
        return
    }
    rf.mu.Lock()
    if rf.LeaderId != rf.me {
        rf.mu.Unlock()
        return
    }
    args := InstallSnapshotArgs{}
    args.Term = rf.CurrentTerm
    args.LeaderId = rf.me
    args.LastIncludedIndex = rf.LastIncludedIndex
    args.LastIncludedTerm = rf.LastIncludedTerm
    args.Snapshot = rf.Checkpoint

    originII := rf.LastIncludedIndex
    originNextIndex := rf.NextIndex[i]
    rf.mu.Unlock()
    reply := InstallSnapshotReply{}

    ok := rf.sendInstallSnapshot(i, &args, &reply)

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if ok {
        if originNextIndex == rf.NextIndex[i] {
            if reply.Success {
                rf.NextIndex[i] = originII + 1
                rf.MatchIndex[i] = originII
            } else {
                rf.CurrentTerm = reply.Term
                oldVoted := rf.VotedFor
                rf.toBeFollower(-1)
                rf.VotedFor = oldVoted
                rf.persist()
            }
        }
    } else {
        go rf.asyncInstallSnapshot(i)
    }
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
    if rf.killed() {
        return -1, -1, false
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()

    isLeader = (rf.LeaderId == rf.me)
    if !isLeader {
        return index, term, isLeader
    }
    entry := LogRecord{}
    entry.Index = rf.lastIndex() + 1
    entry.Term = rf.CurrentTerm
    entry.Command = command
    rf.Log = append(rf.Log, entry)
    rf.NextIndex[rf.me] = rf.lastIndex() + 1
    rf.MatchIndex[rf.me] = rf.lastIndex()
    rf.persist()

    // 异步地并发 AppendEntries RPC 请求来进行日志复制
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        go rf.asyncSendAppendEntries(i)
    }
    index = entry.Index
    term = entry.Term
    MDebug(dClient, "S%d receive a log append request from client. Index = %d, Term = %d\n", rf.me, index, term)
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
    rf.VotedFor = rf.me
    rf.VoteCount = 1
    rf.persist()
    go rf.ticketElectionTimeout()
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        MDebug(dVote, "S%d send vote reqeust to S%d.\n", rf.me, i)
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
                MDebug(dVote, "S%d is going to take part in candidate!\n", rf.me)
                rf.toBeACandidate()
            } else {
                rf.ReceivedAppendEntries = false
            }
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) ticketElectionTimeout() {
    if rf.killed() == false {
        random_number := rand.Intn(500 - 300) + 300
        time.Sleep(time.Duration(random_number) * time.Millisecond)

        rf.mu.Lock()
        defer rf.mu.Unlock()
        if rf.Status == Candidate {
            MDebug(dVote, "S%d's Election Timer timeout!\n", rf.me)
            rf.toBeACandidate()
        } else if rf.Status == Follower {
        }
    }
}

func (rf *Raft) sendHeartBeat() {
    if rf.killed() == false {
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
        time.Sleep(150 * time.Millisecond) // 每 130ms 发一次心跳包
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
    rf.MatchIndex = make(map[int] int)
    for idx, _ := range(rf.peers) {
        rf.MatchIndex[idx] = 0
    }
    rf.applyCh = applyCh
    rf.CVNotifyClient = sync.NewCond(&rf.mu)
    rf.CVSyncCommitIndex = sync.NewCond(&rf.mu)
    // 2B end
    rf.LastIncludedIndex = 0
    rf.LastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    // assert
    if rf.Log[0].Index != rf.LastIncludedIndex || rf.Log[0].Term != rf.LastIncludedTerm {
        MDebug(dError, "First log entry is not right!\n")
        os.Exit(1)
    }
    for idx, _ := range(rf.peers) {
        rf.NextIndex[idx] = rf.lastIndex() + 1
    }
    rf.LastApplied = rf.LastIncludedIndex
    rf.CommitIndex = rf.LastIncludedIndex
    MDebug(dPersist, "S%d Restore from disk! rf.CurrentTerm = %d, rf.VotedFor = %d, size of rf.Log = %d\n", rf.me, rf.CurrentTerm, rf.VotedFor, len(rf.Log))

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.sendHeartBeatPeriodically()
    go rf.notifyClient()
    go rf.sendSyncCommitIndexPeriodically()

	return rf
}

func (rf *Raft) GetLeaderId() int {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.LeaderId
}
