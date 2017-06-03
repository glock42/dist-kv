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
    //"strconv"
    "bytes"
    "encoding/gob"
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

    applyCh     chan ApplyMsg
    voteCount   int
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

func (rf *Raft) getLastEntry() Entry {
    return rf.logs[len(rf.logs) - 1]
}

func (rf *Raft) getLastIndex() int {
    var lastIndex int
    if len(rf.logs) == 1 {
        lastIndex, _ = parseSnapshot(rf.persister.snapshot)
    } else {
        lastIndex = rf.getLastEntry().Index
    }
    return lastIndex
}

func (rf *Raft) getLastTerm() int {
    var lastTerm int
    if len(rf.logs) == 1 {
        _, lastTerm = parseSnapshot(rf.persister.snapshot)
    } else {
        lastTerm = rf.getLastEntry().Term
    }
    return lastTerm
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
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.logs)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
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
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
    d.Decode(&rf.logs)
}

//func (rf *Raft) installSnapshot() {
//    println("install snapshot---")
//    w := new(bytes.Buffer)
//    e := gob.NewEncoder(w)
//    baseIndex := rf.logs[1].Index
//    index := rf.lastApplied - baseIndex + 1
//    if index > 0 {
//        lastAppliedEntry := rf.logs[index];
//        e.Encode(lastAppliedEntry.Index)
//        e.Encode(lastAppliedEntry.Term)
//        //rf.compactLogs(index)
//        data := w.Bytes()
//        rf.persister.SaveSnapshot(data)
//    }
//
//}
//
//func (rf *Raft) compactLogs(i int) {
//    fmt.Printf("%d before compact logs  len(logs) %d, rf.lastApplied %d, rf.commitindex %d\n", rf.me, len(rf.logs), rf.lastApplied, rf.commitIndex)
//    rf.logs = append(rf.logs[0:1], rf.logs[i + 1:]...)
//    fmt.Printf("%d after compact logs  len(logs) %d, rf.lastApplied %d, rf.commitindex %d\n", rf.me, len(rf.logs), rf.lastApplied, rf.commitIndex)
//}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term         int
    CandidateId  int

    LastLogIndex int
    LastLogTerm  int
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

type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int

    LastIncludedIndex int
    LastIncludedTerm  int

    Data              []byte
}

type InstallSnapshotReply struct {
    Term    int
    Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (2A, 2B).
    rf.lock()
    defer rf.unLock()
    defer rf.persist()
    //println(strconv.Itoa(args.CandidateId) + " term " + strconv.Itoa(args.Term) + " request " + strconv.Itoa(rf.me) + " term " + strconv.Itoa(rf.currentTerm))
    if rf.votedFor != -1 || args.Term < rf.currentTerm {
        reply.VoteGranted = false
    } else {
        rf.currentTerm = args.Term
        rf.status = FOLLOWER
        rf.timeReset <- true
        receiverLastIndex := rf.getLastIndex()
        receiverLastTerm := rf.getLastTerm()
        if (args.LastLogTerm != receiverLastTerm && args.LastLogTerm >= receiverLastTerm ) ||
                (args.LastLogTerm == receiverLastTerm && args.LastLogIndex >= receiverLastIndex) {
            rf.votedFor = args.CandidateId
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    }
    reply.Term = rf.currentTerm

    //println(strconv.Itoa(args.CandidateId) + " request " + strconv.Itoa(rf.me) + " over")
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.lock()
    defer rf.unLock()
    defer rf.persist()
    if args.IsHeartbeat {
        //println(strconv.Itoa(rf.me) + " recive heartbeat")
    } else {
        //println(strconv.Itoa(rf.me) + " recive append entry")
    }
    //println("rf.currentTerm" + strconv.Itoa(rf.currentTerm))
    //println("args.Term" + strconv.Itoa(args.Term))
    if args.Term >= rf.currentTerm {
        rf.currentTerm = args.Term
        reply.Term = rf.currentTerm;
        rf.status = FOLLOWER
        rf.timeReset <- true
        rf.votedFor = -1

        if len(rf.logs) == 1 {
            prevIndex, prevTerm := parseSnapshot(rf.persister.ReadSnapshot())
            if prevIndex != args.PrevLogIndex || prevTerm != args.PrevLogTerm {
                reply.Success = false
                return
            }
        } else {
            baseIndex := rf.logs[1].Index;
            index := args.PrevLogIndex - baseIndex + 1
            if index >= len(rf.logs) || rf.logs[index].Term != args.PrevLogTerm {
                reply.Success = false
                return
            }
        }

        hasSameEntry := false
        for i := 1; i < len(rf.logs); i++ {
            entry := rf.logs[i]
            if entry.Index == args.Entries.Index && entry.Term != args.Entries.Term {
                rf.logs = rf.logs[:i]
                break
            } else if (entry.Index == args.Entries.Index &&entry.Term == args.Entries.Term) {
                hasSameEntry = true
            }
        }

        if !args.IsHeartbeat && !hasSameEntry {
            rf.logs = append(rf.logs, args.Entries)
            //println(strconv.Itoa(rf.me) + " append " + strconv.Itoa(args.Entries.Index) + " " + strconv.Itoa(args.Entries.Term) + " " + strconv.Itoa(args.Entries.Command.(int)))
        }

        if rf.commitIndex < args.LeaderCommitIndex {
            if args.LeaderCommitIndex < rf.getLastIndex() {
                rf.commitIndex = args.LeaderCommitIndex
            } else {
                rf.commitIndex = rf.getLastIndex()
            }
        }

        //println("rf " + strconv.Itoa(rf.me) + " len logs " + strconv.Itoa(len(rf.logs)))
        //println("rf " + strconv.Itoa(rf.me) + " commitIndex " + strconv.Itoa(rf.commitIndex))
        //println("rf " + strconv.Itoa(rf.me) + " last applied " + strconv.Itoa(rf.lastApplied))
        reply.Success = true
    } else {
        reply.Term = rf.currentTerm;
        reply.Success = false
        reply.TermNotRight = true
    }
}

//func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
//
//    rf.lock()
//    defer rf.unLock()
//    if args.Term < rf.currentTerm {
//        reply.Term = rf.currentTerm
//        reply.Success = false
//        return
//    }
//
//    fmt.Printf("------------------- %d received snapshot -------------------\n", rf.me)
//    rf.currentTerm = args.Term
//    reply.Term = rf.currentTerm;
//    rf.status = FOLLOWER
//    rf.timeReset <- true
//    rf.votedFor = -1
//
//    rf.persister.SaveSnapshot(args.Data)
//    //rf.compactLogs(args.LastIncludedIndex)
//    //rf.lastApplied = args.LastIncludedIndex
//    //rf.commitIndex = args.LastIncludedIndex
//    msg := ApplyMsg{UseSnapshot:true, Snapshot: args.Data}
//    rf.applyCh <- msg
//    println("snapshot apply**************************")
//    reply.Success = true
//}

func (rf *Raft) applyCommand(entry Entry) {
    applyCh := ApplyMsg{}
    applyCh.Index = entry.Index
    applyCh.Command = entry.Command
    rf.applyCh <- applyCh
    //println(strconv.Itoa(rf.me) + " apply ***************************** " + strconv.Itoa(entry.Command.(int)))
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
    rf.lock()
    defer rf.unLock()
    //fmt.Printf("%d send request to %d\n", rf.me, index)
    if ok && reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.status = FOLLOWER
        rf.persist()
        return ok
    }
    if ok && reply.VoteGranted && rf.status != LEADER {
        rf.voteCount++
        if rf.voteCount > len(rf.peers) / 2 {
            //println(strconv.Itoa(rf.me) + " get leader ----------")
            rf.beLeader <- true
        }
    }
    return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

    if !ok {
        return ok
    }
    if !args.IsHeartbeat {
        //fmt.Printf("%d send entry to %d \n", rf.me, server)
    }
    rf.lock()
    defer rf.unLock()
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        //fmt.Printf("---------------------------------------------------------------------- leader %d become follower\n", rf.me)
        rf.status = FOLLOWER
        rf.votedFor = -1
        rf.persist()
        return ok
    }
    if args.IsHeartbeat {
        return ok
    }
    if reply.Success {
        rf.nextIndex[server]++
        rf.matchIndex[server] = rf.nextIndex[server] - 1
    } else {
        if !reply.TermNotRight {
            //println("retry")
            rf.nextIndex[server]--
        }
    }
    return ok
}

//func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
//    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
//    if !ok {
//        return ok
//    }
//
//    if reply.Term > rf.currentTerm {
//        rf.currentTerm = reply.Term
//        fmt.Printf("---------------------------------------------------------------------- leader %d become follower\n", rf.me)
//        rf.status = FOLLOWER
//        rf.votedFor = -1
//        return ok
//    }
//
//    if reply.Success {
//        //rf.nextIndex[server] = args.LastIncludedIndex + 1
//        //rf.matchIndex[server] = args.LastIncludedIndex
//    }
//    return ok
//}

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
        //println(strconv.Itoa(rf.me) + " ------ start ----- cmd " + strconv.Itoa(command.(int)))
        index = rf.getLastIndex() + 1
        entry := Entry{index, rf.currentTerm, command}
        rf.logs = append(rf.logs, entry)
        rf.persist()
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
    rf.persist()
    rf.unLock()
    go func() {
        broadcastRequestVote(rf)
    }()
}

func broadcastRequestVote(rf *Raft) {
    rf.lock()
    defer rf.unLock()
    rf.voteCount = 1
    for i := range rf.peers {
        if i != rf.me && rf.status == CANDIDATE {
            requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastIndex(), rf.getLastTerm()}
            result := RequestVoteReply{};
            go func(server int) {
                rf.sendRequestVote(server, &requestVoteArgs, &result);
            }(i)
        }
    }
}

func parseSnapshot(snapshot []byte) (int, int) {
    if snapshot == nil || len(snapshot) < 1 {
        return 0, 0
    }

    var lastIncludeIndex int
    var lastIncludeTerm int

    r := bytes.NewBuffer(snapshot)
    d := gob.NewDecoder(r)
    d.Decode(&lastIncludeIndex)
    d.Decode(&lastIncludeTerm)
    return lastIncludeIndex, lastIncludeTerm
}
func broadcastAppendEntries(rf *Raft) {
    //println("------ start broadcast append entry ------")
    rf.lock()
    defer rf.unLock()
    for i := range rf.peers {
        if i != rf.me && rf.status == LEADER {
            appendEntriesArgs := AppendEntriesArgs{};
            appendEntriesArgs.Term = rf.currentTerm
            appendEntriesArgs.LeaderId = rf.me
            //println("logs size " + strconv.Itoa(len(rf.logs)))
            //println("commit index " + strconv.Itoa(rf.commitIndex))
            //fmt.Printf("last log's Index %d, nextIndex[%d] %d\n", rf.logs[len(rf.logs) - 1].Index, i, rf.nextIndex[i])

            baseIndex := rf.getBaseIndex()
            nextIndex := rf.nextIndex[i] - baseIndex

            if rf.nextIndex[i] > baseIndex {
                if rf.getLastIndex() - baseIndex >= nextIndex {
                    appendEntriesArgs.Entries = rf.logs[nextIndex]
                    appendEntriesArgs.IsHeartbeat = false
                } else {
                    appendEntriesArgs.Entries = Entry{}
                    appendEntriesArgs.IsHeartbeat = true
                }
                var prevLogIndex int
                var prevLogTerm int
                if nextIndex - 1 >= 1 && nextIndex - 1 < len(rf.logs) {
                    prevLogIndex = rf.logs[nextIndex - 1].Index
                    prevLogTerm = rf.logs[nextIndex - 1].Term
                } else {
                    prevLogIndex, prevLogTerm = parseSnapshot(rf.persister.ReadSnapshot())
                }
                appendEntriesArgs.LeaderCommitIndex = rf.commitIndex
                appendEntriesArgs.PrevLogIndex = prevLogIndex
                appendEntriesArgs.PrevLogTerm = prevLogTerm
                result := AppendEntriesReply{}
                result.TermNotRight = false
                go func(server int) {
                    rf.sendAppendEntries(server, &appendEntriesArgs, &result)
                }(i)
            }
            //} else {
            //    installSnapshotArgs := InstallSnapshotArgs{}
            //    installSnapshotArgs.Term = rf.currentTerm
            //    installSnapshotArgs.LeaderId = rf.me
            //    installSnapshotArgs.LastIncludedIndex,
            //            installSnapshotArgs.LastIncludedTerm = parseSnapshot(rf.persister.snapshot)
            //    installSnapshotArgs.Data = rf.persister.snapshot
            //    installSnapshotReply := InstallSnapshotReply{}
            //    installSnapshotReply.Success = false
            //
            //    go func() {
            //        rf.sendInstallSnapshot(i, &installSnapshotArgs, &installSnapshotReply)
            //    }()
            //}

        }
    }
}

func (rf *Raft) lock() {
    rf.mu.Lock()
}

func (rf *Raft) unLock() {
    rf.mu.Unlock()
}

func (rf *Raft) getBaseIndex() int {
    var baseIndex int
    baseIndex, _ = parseSnapshot(rf.persister.snapshot)
    return baseIndex
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

    rf.applyCh = applyCh

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    go func(rf *Raft) {
        for {
            rf.lock()
            status := rf.status
            //println(strconv.Itoa(rf.me) + " rf.commitIndex " + strconv.Itoa(rf.commitIndex) + " rf.lastApplied " + strconv.Itoa(rf.lastApplied))
            baseIndex := rf.getBaseIndex()
            lastAppliedIndex := rf.lastApplied - baseIndex

            //println("rf " + strconv.Itoa(rf.me) + " len logs " + strconv.Itoa(len(rf.logs)))
            //println("rf " + strconv.Itoa(rf.me) + " commitIndex " + strconv.Itoa(rf.commitIndex))
            //println("rf " + strconv.Itoa(rf.me) + " last applied " + strconv.Itoa(rf.lastApplied))

            if rf.lastApplied < rf.commitIndex && lastAppliedIndex + 1 < len(rf.logs) {
                //println(lastAppliedIndex)
                rf.applyCommand(rf.logs[lastAppliedIndex + 1])
            }
            rf.unLock()
            switch status {
            case FOLLOWER:
                //println(strconv.Itoa(rf.me) + " start follower")
                    select {
                    case <-time.After(getRandomExpireTime()):
                        rf.lock()
                    //println(strconv.Itoa(rf.me) + " be Candidate")
                        rf.votedFor = -1
                        rf.status = CANDIDATE
                        rf.persist()
                        rf.unLock()
                    case <-rf.timeReset:
                    }
            case LEADER:
                rf.lock()
                baseIndex := rf.getBaseIndex()
                lastIndex := rf.getLastIndex()
                //println(baseIndex)
                //println(lastIndex)
                for N := rf.commitIndex + 1; N <= lastIndex; N++ {
                    count := 1
                    for i := 0; i < len(rf.matchIndex); i++ {
                        //fmt.Printf("%d ", rf.matchIndex[i])
                        if rf.me != i && rf.matchIndex[i] >= N && rf.logs[N - baseIndex].Term == rf.currentTerm {
                            count++
                        }
                        if count > len(rf.matchIndex) / 2 {
                            rf.commitIndex = N
                        }
                    }
                    //fmt.Printf("\n")
                }
                rf.persist()
                rf.unLock()
                broadcastAppendEntries(rf)
                time.Sleep(time.Duration(HEARTBEAT_TIME) * time.Millisecond)
            case CANDIDATE:
                election(rf)
                    select {
                    case <-time.After(getRandomExpireTime()):
                        rf.lock()
                        rf.status = FOLLOWER
                        rf.votedFor = -1
                        rf.persist()
                        rf.unLock()
                    case <-rf.timeReset:
                        rf.lock()
                        rf.status = FOLLOWER
                        rf.votedFor = -1
                        rf.persist()
                        rf.unLock()
                    case <-rf.beLeader:
                        rf.lock()
                        rf.status = LEADER
                        rf.nextIndex = make([]int, len(rf.peers))
                        for i := 0; i < len(rf.peers); i++ {
                            rf.nextIndex[i] = rf.getLastIndex() + 1
                        }
                        rf.matchIndex = make([]int, len(rf.peers))
                        for i := 0; i < len(rf.peers); i++ {
                            rf.matchIndex[i] = 0
                        }
                        rf.unLock()
                    //println(strconv.Itoa(rf.me) + " be leader ------------")
                    }
            }
        }

    }(rf)

    return rf
}
