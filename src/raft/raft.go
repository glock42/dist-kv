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
    "sync"
)
import (
    "labrpc"
    "bytes"
    "encoding/gob"
    "math/rand"
    "strconv"
    //"net"
    "time"
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
    mu           sync.Mutex          // Lock to protect shared access to this peer's state
    peers        []*labrpc.ClientEnd // RPC end points of all peers
    persister    *Persister          // Object to hold this peer's persisted state
    me           int                 // this peer's index into peers[]

                                     // Your data here (2A, 2B, 2C).
                                     // Look at the paper's Figure 2 for a description of what
                                     // state a Raft server must maintain.
    logs         []Entry

    commitIndex  int
    lastApplied  int

    nextIndex    []int
    matchIndex   []int

    currentTerm  int
    votedFor     int

    status       Status
    beLeader     chan bool
    timeToCommit chan bool
    grantVote    chan bool
    getHeartBeat chan bool

    applyCh      chan ApplyMsg
    voteCount    int

    base         int
}

type Entry struct {
    Index   int
    Term    int
    Command interface{}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    return rf.currentTerm,  rf.status == LEADER
}

func (rf *Raft) getLastEntry() Entry {
    return rf.logs[len(rf.logs) - 1]
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

    Entries           []Entry
    LeaderCommitIndex int

    PrevLogIndex      int
    PrevLogTerm       int
    IsHeartbeat       bool
}

type AppendEntriesReply struct {
    Term      int
    Success   bool
    NextIndex int
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

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.status = FOLLOWER
        rf.votedFor = -1
    }

    reply.Term = rf.currentTerm

    if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
        reply.VoteGranted = false
        return
    }

    receiverLastIndex := rf.getLastEntry().Index
    receiverLastTerm := rf.getLastEntry().Term
    if (args.LastLogTerm > receiverLastTerm ) ||
            (args.LastLogTerm == receiverLastTerm && args.LastLogIndex >= receiverLastIndex) {
        rf.grantVote <- true
        rf.votedFor = args.CandidateId
        reply.VoteGranted = true
    } else {
        reply.VoteGranted = false
    }

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

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
        reply.NextIndex = rf.getLastEntry().Index + 1
        return
    }

    rf.getHeartBeat <- true

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.status = FOLLOWER
        rf.votedFor = -1
    }

    reply.Term = rf.currentTerm

    if args.PrevLogIndex > rf.getLastEntry().Index || args.PrevLogIndex < rf.base {
        reply.Success = false
        reply.NextIndex = rf.getLastEntry().Index + 1
        return
    }
    if !args.IsHeartbeat {

        //Log2("raft.go: server %d AppendEntries, rf.base: %d, len(rf.logs): %d, args.PrevLogIndex: %d, " +
        //    "rf.Term: %d, args.Term: %d, rf.status: %d\n",
        //    rf.me, rf.base, len(rf.logs), args.PrevLogIndex, rf.currentTerm, args.Term, rf.status)

		Log2("raft.go: server %d AppendEntries, rf.base: %d, len(rf.logs): %d, args.PrevLogIndex: %d, " +
			"rf.Term: %d, args.Term: %d, rf.status: %d, prev log Index: %d, prev log term: %d\n",
			rf.me, rf.base, len(rf.logs), args.PrevLogIndex, rf.currentTerm, args.Term, rf.status,
			rf.logs[rf.getLogsIdx(args.PrevLogIndex)].Index, rf.logs[rf.getLogsIdx(args.PrevLogIndex)].Term)
    } else {
        Log2("raft.go: server %d Heartbeat, rf.base: %d, len(rf.logs): %d, " +
            "args.LeaderCommitIndex: %d, rf.commitIndex: %d, rf.applyIndex: %d, args.PrevLogIndex: %d, rf.status: %d\n",
			rf.me, rf.base, len(rf.logs), args.LeaderCommitIndex, rf.commitIndex, rf.lastApplied, args.PrevLogIndex, rf.status)
    }


    if rf.logs[rf.getLogsIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
        reply.Success = false
        // bypass all of the conflicting entries in this term
        term := rf.logs[rf.getLogsIdx(args.PrevLogIndex)].Term
        for i := args.PrevLogIndex - 1; i >= rf.base; i-- {
            if rf.logs[rf.getLogsIdx(i)].Term != term {
                reply.NextIndex = i + 1
                break
            }
        }
        return
    }

    rf.logs = rf.logs[: rf.getLogsIdx(args.PrevLogIndex) + 1]
    if !args.IsHeartbeat {
        rf.logs = append(rf.logs, args.Entries...)
        //println(strconv.Itoa(rf.me) + " append " + strconv.Itoa(len(args.Entries)) + " entry")
        Log2("raft.go: server %d AppendEntries success, rf.base: %d, len(rf.logs): %d, args.PrevLogIndex: %d \n",
            rf.me, rf.base, len(rf.logs), args.PrevLogIndex)
    }

    if rf.commitIndex < args.LeaderCommitIndex {
        if args.LeaderCommitIndex < rf.getLastEntry().Index {
            rf.commitIndex = args.LeaderCommitIndex
        } else {
            rf.commitIndex = rf.getLastEntry().Index
        }
        rf.timeToCommit <- true
    }

    //println("rf " + strconv.Itoa(rf.me) + " len logs " + strconv.Itoa(len(rf.logs)))
    //println("rf " + strconv.Itoa(rf.me) + " commitIndex " + strconv.Itoa(rf.commitIndex))
    //println("rf " + strconv.Itoa(rf.me) + " last applied " + strconv.Itoa(rf.lastApplied))
    reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.lock()
    defer rf.unLock()
    defer rf.persist()

    reply.Term = rf.currentTerm

    if args.Term < rf.currentTerm {
        return
    }

    if args.Term > rf.currentTerm {
        rf.status = FOLLOWER
        rf.votedFor = -1
    }

    Log2("raft.go: InstallSnapshot, args.LastIncludeIndex: %d, len(rf.logs): %d, rf.base: %d \n",
        args.LastIncludedIndex, len(rf.logs), rf.base)


    newLogs := make([]Entry, 1)
    newLogs[0].Index = args.LastIncludedIndex
    newLogs[0].Term  = args.LastIncludedTerm

    if rf.getLogsIdx(args.LastIncludedIndex) > 0 && rf.getLogsIdx(args.LastIncludedIndex) < len(rf.logs)  {
        newLogs = append(newLogs, rf.logs[rf.getLogsIdx(args.LastIncludedIndex) + 1:]...)
    }

    rf.base = args.LastIncludedIndex
    rf.logs = newLogs

    rf.readSnapshot(args.Data)
    Log2("raft.go: server %d, truncate logs, rf.logs, len: %d\n", rf.me, len(rf.logs))

    rf.lastApplied =  args.LastIncludedIndex
    rf.commitIndex =  args.LastIncludedIndex

    rf.persister.SaveSnapshot(args.Data)
    Log2("raft.go: InstallSnapshot over, rf.lastApplied: %d, rf.commitIndex: %d, len(rf.logs): %d, rf.base: %d \n",
        rf.lastApplied, rf.commitIndex, len(rf.logs), rf.base)
}

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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
    rf.lock()
    defer rf.unLock()
    if !ok {
        return ok
    }

    if args.Term != rf.currentTerm {
        return ok
    }

    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.status = FOLLOWER
        rf.persist()
        return ok
    }

    rf.nextIndex[server] =  args.LastIncludedIndex + 1
    rf.matchIndex[server] = args.LastIncludedIndex

    return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    rf.lock()
    defer rf.unLock()
    //fmt.Printf("%d send request to %d\n", rf.me, index)
    if !ok {
        return ok
    }
    if rf.status != CANDIDATE {
        return ok
    }
    if args.Term != rf.currentTerm {
        return ok
    }
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.status = FOLLOWER
        rf.persist()
        return ok
    }
    if reply.VoteGranted && rf.status == CANDIDATE {
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
    rf.lock()
    defer rf.unLock()

    if !ok {
        return ok
    }

    if rf.status != LEADER {
        return ok
    }

    if args.Term != rf.currentTerm {
        return ok
    }

    if !args.IsHeartbeat {
        //fmt.Printf("%d send entry to %d \n", rf.me, server)
    }

    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        Log2("raft.go: server leader %d is stall, become follower\n", rf.me)
        rf.status = FOLLOWER
        rf.votedFor = -1
        rf.persist()
        return ok
    }
    if reply.Success {
        if !args.IsHeartbeat {
            rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].Index + 1
            rf.matchIndex[server] = rf.nextIndex[server] - 1
        }
    } else {
        rf.nextIndex[server] = reply.NextIndex
    }
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

    //Log("server %d Start", rf.me)
    rf.lock()
    // Your code here (2B).
    if rf.status == LEADER {
        //println(strconv.Itoa(rf.me) + " ------ start ----- cmd " + strconv.Itoa(command.(int)))
        index = rf.getLastEntry().Index + 1
        entry := Entry{index, rf.currentTerm, command}
        rf.logs = append(rf.logs, entry)
        rf.persist()
    } else {
        isLeader = false
    }

    term = rf.currentTerm
    rf.unLock()
    //Log("server %d Start over", rf.me)
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
    Log2("raft.go: server %d be killed\n", rf.me)
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
            requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastEntry().Index, rf.getLastEntry().Term}
            result := RequestVoteReply{};
            go func(server int) {
                rf.sendRequestVote(server, &requestVoteArgs, &result);
            }(i)
        }
    }
}

func broadcastAppendEntries(rf *Raft) {
    //println("------ start broadcast append entry ------")
    rf.lock()
    defer rf.unLock()

    for N := rf.commitIndex + 1; N <= rf.getLastEntry().Index; N++ {
        count := 1
        for i := 0; i < len(rf.matchIndex); i++ {
            //fmt.Printf("%d ", rf.matchIndex[i])
            if rf.me != i && rf.matchIndex[i] >= N && rf.logs[rf.getLogsIdx(N)].Term == rf.currentTerm {
                count++
            }
            if count > len(rf.matchIndex) / 2 {
            	Log("server %d, update commitIndex {old: %d, new: %d} \n", rf.me, rf.commitIndex, N)
                rf.commitIndex = N
                break
            }
        }
        //fmt.Printf("\n")
    }
    if rf.lastApplied < rf.commitIndex && rf.lastApplied+1 < rf.base + len(rf.logs) {
        rf.timeToCommit <- true
    }

    for i := range rf.peers {
        if i != rf.me && rf.status == LEADER {
            if rf.nextIndex[i] > rf.base {

                appendEntriesArgs := AppendEntriesArgs{}
                appendEntriesArgs.Term = rf.currentTerm
                appendEntriesArgs.LeaderId = rf.me
                //println("logs size " + strconv.Itoa(len(rf.logs)))
                //println("commit index " + strconv.Itoa(rf.commitIndex))
                //fmt.Printf("last log's Index %d, nextIndex[%d] %d\n", rf.logs[len(rf.logs) - 1].Index, i, rf.nextIndex[i])

                //baseIndex := rf.getBaseIndex()
                nextIndex := rf.nextIndex[i]
                //fmt.Printf("nextIndex[%d] %d\n", i, nextIndex)
                //if rf.nextIndex[i] > baseIndex {
                if rf.getLastEntry().Index >= nextIndex {
                    appendEntriesArgs.Entries = make([]Entry, len(rf.logs[rf.getLogsIdx(nextIndex):]))
                    copy(appendEntriesArgs.Entries, rf.logs[rf.getLogsIdx(nextIndex):])
                    //appendEntriesArgs.Entries = rf.logs[nextIndex:]
                    appendEntriesArgs.IsHeartbeat = false
                } else {
                    appendEntriesArgs.Entries = make([]Entry, 0)
                    appendEntriesArgs.IsHeartbeat = true
                }

                appendEntriesArgs.PrevLogIndex = nextIndex - 1
                appendEntriesArgs.PrevLogTerm = rf.logs[rf.getLogsIdx(appendEntriesArgs.PrevLogIndex)].Term
                if !appendEntriesArgs.IsHeartbeat {
                    Log2("raft.go: server %d broadcastAppendEntries to %d, nextIndex: %d, PrevLogIndex: %d, commitIndex: %d " +
                        "rf.base: %d, len(rf.logs): %d, rf.status: %d, prevLog Term: %d\n" ,
                        rf.me, i, nextIndex, appendEntriesArgs.PrevLogIndex, rf.commitIndex, rf.base, len(rf.logs),
                        rf.status, appendEntriesArgs.PrevLogTerm)
                } else {
                    Log2("raft.go: server %d broadcastHeartbeat to %d, commitIndex: %d, applyIndex: %d " +
                        "rf.base: %d, len(rf.logs): %d, rf.status: %d\n" ,
                        rf.me, i, rf.commitIndex, rf.lastApplied, rf.base, len(rf.logs),
                        rf.status)
                }
                appendEntriesArgs.LeaderCommitIndex = rf.commitIndex
                result := AppendEntriesReply{}
                go func(server int) {
                    rf.sendAppendEntries(server, &appendEntriesArgs, &result)
                }(i)
            } else {
            	installArgs := InstallSnapshotArgs{}
            	installReply := InstallSnapshotReply{}

            	installArgs.Term = rf.currentTerm
            	installArgs.LeaderId = rf.me
            	installArgs.LastIncludedIndex = rf.logs[0].Index
            	installArgs.LastIncludedTerm = rf.logs[0].Term
            	installArgs.Data = rf.persister.ReadSnapshot()
            	Log2("raft.go: server %d broadcastInstallSnapshot to %d, lastIncludeIndex: %d, lastIncludeTerm: %d\n" ,
            	    rf.me, i, installArgs.LastIncludedIndex, installArgs.LastIncludedTerm)
            	go func(server int) {
                    rf.sendInstallSnapshot(server, &installArgs, &installReply)
                }(i)
                // snapshot
            }
        }
    }
}

func (rf *Raft) lock() {
    rf.mu.Lock()
}

func (rf *Raft) unLock() {
    rf.mu.Unlock()
}

func (rf *Raft) SnapShot(data []byte, lastIncludeIndex int) {
    rf.lock()
    defer rf.unLock()
    Log2("raft.go: server %d, start snapshot, lastIncludeIndex: %d, len(rf.logs): %d, rf.base: %d\n",
        rf.me, lastIncludeIndex, len(rf.logs), rf.base)

    if lastIncludeIndex > rf.base {

        newLogs := make([]Entry, 1)
        lastIncludeEntry := rf.logs[rf.getLogsIdx(lastIncludeIndex)]

        newLogs[0].Index = lastIncludeEntry.Index
        newLogs[0].Term = lastIncludeEntry.Term
        newLogs = append(newLogs, rf.logs[rf.getLogsIdx(lastIncludeIndex)+1:]...)
        rf.base = lastIncludeIndex
        rf.logs = newLogs

        Log2("raft.go: server %d, truncate logs, rf.logs, len: %d\n", rf.me, len(rf.logs))

        rf.lastApplied = lastIncludeIndex
        rf.commitIndex = lastIncludeIndex

        rf.persist()
        rf.persister.SaveSnapshot(data)
    }
    Log2("raft.go: server %d, snapshot over, len(rf.logs): %d, lastApplied: %d, commitIndex: %d, rf.base: %d\n",
        rf.me, len(rf.logs), rf.lastApplied, rf.commitIndex, rf.base)
}

func (rf *Raft) getLogsIdx(raftIndex int) int  {
    return raftIndex - rf.base
}

func (rf *Raft) GetRaftStateSize() int {
    return rf.persister.RaftStateSize()
}

func (rf *Raft) readSnapshot(snapshot []byte) {
    msg := ApplyMsg{}
    msg.UseSnapshot = true
    msg.Snapshot = snapshot

    go func() {
        rf.applyCh <- msg
    }()
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
    rf.timeToCommit = make(chan bool, 1)
    rf.grantVote = make(chan bool, 1)
    rf.getHeartBeat = make(chan bool, 1)

    rf.logs = make([]Entry, 1)
    //rf.nextIndex = make([]int, 0)
    //rf.matchIndex = [len(rf.peers)]int{}

    rf.applyCh = applyCh

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())
    rf.readSnapshot(persister.ReadSnapshot())
    rf.base = rf.logs[0].Index
    rf.commitIndex = rf.logs[0].Index
    rf.lastApplied = rf.logs[0].Index
    go func(rf *Raft) {
        for {
            switch rf.status {
            case FOLLOWER:
                Log("server " +  strconv.Itoa(rf.me) + " start follower")
                select {
                case <-time.After(getRandomExpireTime()):
                    rf.lock()
                    //Log("server " + strconv.Itoa(rf.me) + " be Candidate")
                    rf.votedFor = -1
                    rf.status = CANDIDATE
                    rf.persist()
                    rf.unLock()
                case <-rf.getHeartBeat:
                case <-rf.grantVote:
                }
            case LEADER:
            	//Log("server %d, broadcast append entries\n", rf.me, )
                broadcastAppendEntries(rf)
                time.Sleep(time.Duration(HEARTBEAT_TIME) * time.Millisecond)
            case CANDIDATE:
                election(rf)
				select {
				case <-time.After(getRandomExpireTime()):
				case <-rf.getHeartBeat:
					rf.lock()
                    //println(strconv.Itoa(rf.me) + "candidate get heartbeat and be follower")
					rf.status = FOLLOWER
					rf.votedFor = -1
					rf.persist()
					rf.unLock()
				case <-rf.beLeader:
					rf.lock()
					rf.status = LEADER
                    Log2("server " + strconv.Itoa(rf.me) + " becomes leader")
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.getLastEntry().Index + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = rf.logs[0].Index
					}
					rf.unLock()
					//println(strconv.Itoa(rf.me) + " be leader ------------")
				}
            }
        }

    }(rf)

    go func(rf *Raft) {
        for {
            select {
            case <-rf.timeToCommit:

                //Log("server %d apply command\n", rf.me)

                rf.lock()
            //println(strconv.Itoa(rf.me) + " rf.commitIndex " + strconv.Itoa(rf.commitIndex) + " rf.lastApplied " + strconv.Itoa(rf.lastApplied))
            //println("rf " + strconv.Itoa(rf.me) + " len logs " + strconv.Itoa(len(rf.logs)))
            //println("rf " + strconv.Itoa(rf.me) + " commitIndex " + strconv.Itoa(rf.commitIndex))
            //println("rf " + strconv.Itoa(rf.me) + " last applied " + strconv.Itoa(rf.lastApplied))

                for rf.lastApplied < rf.commitIndex && rf.getLogsIdx(rf.lastApplied + 1) < len(rf.logs) {
                    //println(lastAppliedIndex)
                    Log2("raft.go: server %d applyCommand, len(log): %d, rf.lastApplied: %d, " +
                        "rf.commitIndex: %d, rf.base: %d\n",
                        rf.me, len(rf.logs), rf.lastApplied, rf.commitIndex, rf.base)
                    rf.applyCommand(rf.logs[rf.getLogsIdx(rf.lastApplied) + 1])
                }
                rf.unLock()
                //Log("server %d apply command over\n", rf.me)
            }
        }
    }(rf)

    return rf
}
