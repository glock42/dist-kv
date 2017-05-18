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

import "sync"
import (
    "labrpc"
    //"net"
    "time"
    "math/rand"
    "strconv"
)

// import "bytes"
// import "encoding/gob"


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
    Index       int
    Command     interface{}
    UseSnapshot bool   // ignore for lab2; only used in lab3
    Snapshot    []byte // ignore for lab2; only used in lab3
}

type Status int

const (
    FOLLOWER Status = iota // value --> 0
    CANDIDATE              // value --> 1
    LEADER                 // value --> 2
)

const HEARTBEAT_TIME int = 50

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu          sync.Mutex          // Lock to protect shared access to this peer's state
    peers       []*labrpc.ClientEnd // RPC end points of all peers
    persister   *Persister          // Object to hold this peer's persisted state
    me          int                 // this peer's index into peers[]

                                    // Your data here (2A, 2B, 2C).
                                    // Look at the paper's Figure 2 for a description of what
                                    // state a Raft server must maintain.
    logs        []Entry

    commitIndex int
    lastApplied int

    nextIndex   []int
    matchIndex  []int

    currentTerm int
    votedFor    int

    status      Status
    beLeader    chan bool
    timeReset   chan bool

    isStart     bool
    applyCh     chan ApplyMsg
}

type Entry struct {
    Index   int
    Term    int
    Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isLeader bool
    // Your code here (2A).
    rf.lock()
    term = rf.currentTerm
    isLeader = rf.status == LEADER
    rf.unLock()

    return term, isLeader
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
    // e := gob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    // Your code here (2C).
    // Example:
    // r := bytes.NewBuffer(data)
    // d := gob.NewDecoder(r)
    // d.Decode(&rf.xxx)
    // d.Decode(&rf.yyy)
    if data == nil || len(data) < 1 {
        // bootstrap without any state?
        return
    }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term        int
    CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term        int
    VoteGranted bool
}

type AppendEntriesArgs struct {
    Term              int
    LeaderId          int

    Entries           Entry
    LeaderCommitIndex int

    PrevLogIndex      int
    PrevLogTerm       int
    IsHeartbeat       bool
}

type AppendEntriesReply struct {
    Term         int
    Success      bool
    TermNotRight bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.lock()

    //println(strconv.Itoa(args.CandidateId) + " term " + strconv.Itoa(args.Term) + " request " + strconv.Itoa(rf.me) + " term " + strconv.Itoa(rf.currentTerm))
    if rf.votedFor != -1 || args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {
        rf.currentTerm = args.Term
        rf.votedFor = args.CandidateId
        rf.status = FOLLOWER
        rf.timeReset <- true
        reply.VoteGranted = true
    }
    reply.Term = rf.currentTerm

    //println(strconv.Itoa(args.CandidateId) + " request " + strconv.Itoa(rf.me) + " over")
    rf.unLock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.lock()
    defer rf.unLock()
    if args.IsHeartbeat {
        //println(strconv.Itoa(rf.me) + " recive heartbeat")
    } else {
        //println(strconv.Itoa(rf.me) + " recive append entry")
    }
    reply.Term = rf.currentTerm;
    //println("rf.currentTerm" + strconv.Itoa(rf.currentTerm))
    //println("args.Term" + strconv.Itoa(args.Term))
    if args.Term >= rf.currentTerm {
        rf.status = FOLLOWER
        rf.timeReset <- true
        rf.votedFor = -1
        if rf.commitIndex < args.LeaderCommitIndex {
            if args.LeaderCommitIndex < rf.logs[len(rf.logs) - 1].Index {
                rf.commitIndex = args.LeaderCommitIndex
            } else {
                rf.commitIndex = rf.logs[len(rf.logs) - 1].Index
            }
        }

        if !args.IsHeartbeat {
            isFound := false
            for i := 0; i < len(rf.logs); i++ {
                entry := rf.logs[i]
                if entry.Index == args.PrevLogIndex && entry.Term == args.PrevLogTerm {
                    //println(strconv.Itoa(rf.me) + " " + strconv.Itoa(args.PrevLogTerm) + " prevLogTerm " + strconv.Itoa(args.PrevLogIndex) + " args.PrevLogIndex")
                    rf.logs = append(rf.logs, args.Entries)
                    println(strconv.Itoa(rf.me) + " append " + strconv.Itoa(args.Entries.Index) + " " + strconv.Itoa(args.Entries.Term) + " " + strconv.Itoa(args.Entries.Command.(int)))
                    isFound = true
                    break
                }
            }
            if !isFound {
                //println("-----------------" + strconv.Itoa(rf.me) + "not found------------------")
                reply.Success = false
                return
            }
            //println("log length " + strconv.Itoa(len(rf.logs)))
        }

        reply.Success = true
    } else {
        rf.currentTerm = args.Term
        rf.status = FOLLOWER
        reply.Success = false
        //reply.NextIndex = (rf.logs[len(rf.logs) - 1].Index) + 1
        reply.TermNotRight = true
    }
}

func (rf *Raft) applyCommand(entry Entry) {
    applyCh := ApplyMsg{}
    applyCh.Index = entry.Index
    applyCh.Command = entry.Command
    rf.applyCh <- applyCh
    println(strconv.Itoa(rf.me) + " apply ***************************** " + strconv.Itoa(entry.Command.(int)))
    rf.lastApplied ++
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

    rf.lock()
    // Your code here (2B).
    if rf.status == LEADER {
        //println(strconv.Itoa(rf.me) + " ------ start")
        rf.isStart = true
        entry := Entry{len(rf.logs), rf.currentTerm, command}
        rf.logs = append(rf.logs, entry)
        index = entry.Index
    } else {
        isLeader = false
    }

    term = rf.currentTerm
    rf.unLock()
    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    // Your code here, if desired.
}

func getRandomExpireTime() time.Duration {
    return time.Duration(rand.Int63n(300 - 150) + 150) * time.Millisecond
}

func election(rf *Raft) {
    rf.lock()
    rf.currentTerm ++
    rf.votedFor = rf.me
    rf.unLock()
    go func() {
        broadcastRequestVote(rf)
    }()
}

func broadcastRequestVote(rf *Raft) {
    voteCount := 1
    for i := range rf.peers {
        if i != rf.me {
            go func(index int) {
                rf.lock()
                requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me}
                result := RequestVoteReply{};
                rf.unLock()
                ok := rf.sendRequestVote(index, &requestVoteArgs, &result);
                rf.lock()
                if ok && result.VoteGranted && rf.status != LEADER {
                    voteCount++
                    if voteCount > len(rf.peers) / 2 {
                        //println(strconv.Itoa(rf.me) + " get leader ----------")
                        rf.beLeader <- true
                    }
                }
                rf.unLock()
            }(i)
        }
    }
}

func broadcastAppendEntries(rf *Raft) {
    //println("------ start broadcast append entry ------")
    voteCount := 1
    for i := range rf.peers {
        if i != rf.me {
            go func(i int) {
                rf.lock()
                appendEntriesArgs := AppendEntriesArgs{};
                appendEntriesArgs.Term = rf.currentTerm
                appendEntriesArgs.LeaderId = rf.me
                //println("logs size " + strconv.Itoa(len(rf.logs)))
                //println("commit index " + strconv.Itoa(rf.commitIndex))
                if rf.commitIndex <= rf.logs[len(rf.logs) - 1].Index && rf.nextIndex[i] < len(rf.logs) {
                    appendEntriesArgs.Entries = rf.logs[rf.nextIndex[i]]
                    appendEntriesArgs.LeaderCommitIndex = rf.commitIndex
                    appendEntriesArgs.PrevLogIndex = rf.logs[rf.nextIndex[i] - 1].Index
                    appendEntriesArgs.PrevLogTerm = rf.logs[rf.nextIndex[i] - 1].Term
                    appendEntriesArgs.IsHeartbeat = false
                } else {
                    appendEntriesArgs.Entries = Entry{}
                    appendEntriesArgs.LeaderCommitIndex = rf.commitIndex
                    appendEntriesArgs.PrevLogIndex = -1
                    appendEntriesArgs.PrevLogTerm = -1
                    appendEntriesArgs.IsHeartbeat = true
                }
                result := AppendEntriesReply{}
                result.TermNotRight = false
                rf.unLock()
                ok := rf.sendAppendEntries(i, &appendEntriesArgs, &result)
                if !ok {
                    return
                }
                if appendEntriesArgs.IsHeartbeat {
                    rf.lock()
                    voteCount++
                    rf.unLock()
                    return
                }
                if result.Success {
                    rf.lock()
                    voteCount++
                    rf.nextIndex[i]++
                    rf.matchIndex[i]++
                    println("voteCount " + strconv.Itoa(voteCount) + " len(rf.peers) " + strconv.Itoa(len(rf.peers)))
                    println(rf.isStart)
                    if voteCount > len(rf.peers) / 2 && rf.isStart {
                        //rf.applyCommand(appendEntriesArgs.Entries)
                        rf.commitIndex ++
                        println("commitIndex " + strconv.Itoa(rf.commitIndex))
                        if rf.commitIndex == len(rf.logs) - 1 {
                            rf.isStart = false
                        }
                    }
                    rf.unLock()
                } else {
                    rf.lock()
                    if !result.TermNotRight {
                        rf.nextIndex[i]--
                    }
                    rf.unLock()
                }
            }(i)
        }
    }
}

func (rf *Raft) lock() {
    rf.mu.Lock()
}

func (rf *Raft) unLock() {
    rf.mu.Unlock()
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0
    rf.votedFor = -1

    rf.status = FOLLOWER
    rf.beLeader = make(chan bool, 1)
    rf.timeReset = make(chan bool, 1)

    rf.logs = make([]Entry, 1)
    rf.commitIndex = 0
    rf.lastApplied = 0
    //rf.nextIndex = make([]int, 0)
    //rf.matchIndex = [len(rf.peers)]int{}

    rf.isStart = false
    rf.applyCh = applyCh

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    go func(rf *Raft) {
        for {
            rf.lock()
            status := rf.status
            //println(strconv.Itoa(rf.me) + " rf.commitIndex " + strconv.Itoa(rf.commitIndex) + " rf.lastApplied " + strconv.Itoa(rf.lastApplied))
            if rf.lastApplied < rf.commitIndex && rf.lastApplied + 1 < len(rf.logs) {
                rf.applyCommand(rf.logs[rf.lastApplied + 1])
            }
            rf.unLock()
            switch status {
            case FOLLOWER:
                //println(strconv.Itoa(rf.me) + " start follower")
                    select {
                    case <-time.After(getRandomExpireTime()):
                        rf.lock()
                    //println(strconv.Itoa(rf.me) + " be Candidate")
                        rf.status = CANDIDATE
                        rf.unLock()
                    case <-rf.timeReset:
                    }
            case LEADER:
                broadcastAppendEntries(rf)
                time.Sleep(time.Duration(HEARTBEAT_TIME) * time.Millisecond)
            case CANDIDATE:
                election(rf)
                    select {
                    case <-time.After(getRandomExpireTime()):
                        rf.lock()
                        rf.status = FOLLOWER
                        rf.votedFor = -1
                        rf.unLock()
                    case <-rf.timeReset:
                        rf.lock()
                        rf.status = FOLLOWER
                        rf.votedFor = -1
                        rf.unLock()
                    case <-rf.beLeader:
                        rf.lock()
                        rf.status = LEADER
                        rf.nextIndex = make([]int, len(rf.peers))
                        for i := 0; i < len(rf.peers); i++ {
                            rf.nextIndex[i] = rf.logs[len(rf.logs) - 1].Index + 1
                        }
                        rf.matchIndex = make([]int, len(rf.peers))
                        for i := 0; i < len(rf.peers); i++ {
                            rf.matchIndex[i] = 0
                        }
                        rf.unLock()
                        println(strconv.Itoa(rf.me) + " be leader ------------")
                    }
            }
        }

    }(rf)

    return rf
}
