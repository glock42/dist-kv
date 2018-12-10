package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

const (
	GET = "GET"
	PUT = "PUT"
	APPEND = "APPEND"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientId  int64
	ReqId     int64
}

type ApplyReply struct {
	value string
	err   string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store    map[string]string
	executed map[int64]int64
	opChans  map[int]chan ApplyReply
}

func (kv *RaftKV) startAgree(op Op) ApplyReply {

	raft.Log("server.go: server %d startAgree, op: {key: %s, value: %s, op: %s}," +
		" clientId %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)

	index, term, _ :=kv.rf.Start(op)
	kv.mu.Lock()
	opChan, ok := kv.opChans[index]
	if !ok {
		kv.opChans[index] = make(chan ApplyReply, 1)
		opChan= kv.opChans[index]
	}
	kv.mu.Unlock()
	reply := ApplyReply{}
	select {
		case reply = <- opChan:
			curTerm, isLeader := kv.rf.GetState()
			if !isLeader || term != curTerm {
				reply.value = ""
				reply.err = ERROR
			}
		case <-time.After(1000 * time.Millisecond):
			reply.err = ERROR
	}
	raft.Log("server.go: server %d startAgree over, op: {key: %s, value: %s, op: %s}," +
		" clientId %d, reqId: %d, result: %s \n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (kv *RaftKV) apply(op Op) ApplyReply {
	raft.Log("server.go: server %d apply, op: {key: %s, value: %s, op: %s}," +
		" clientId %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := ApplyReply{}
	reply.err = OK
	reply.value = ""
	executedReqId, _ := kv.executed[op.ClientId]
	if op.ReqId > executedReqId {
		if op.Operation == GET {
			v, ok := kv.store[op.Key]
			if ok {
				reply.value = v
			} else {
				reply.err = ErrNoKey
			}
		} else if op.Operation == APPEND {
			v, ok := kv.store[op.Key]
			if ok {
				kv.store[op.Key] = v + op.Value
			} else {
				reply.err = ErrNoKey
			}
		} else {
			kv.store[op.Key] = op.Value
		}
		kv.executed[op.ClientId] = op.ReqId
	} else {
		//raft.Log("server.go: server %d waitToAggre failed, op: {key: %s, value: %s, op: %s}," +
		//	" clientId %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
		if op.Operation == GET {
			reply.value = kv.store[op.Key]
		}
		reply.err = ErrDupReq
	}
	raft.Log("server.go: server %d apply over, op: {key: %s, value: %s, op: %s}," +
		" clientId %d, reqId: %d, err: %s\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	raft.Log("server.go: server %d Get, {key: %s}, isLeader: %t, clientId: %d\n",
		kv.me, args.Key, isLeader, args.Id)

	reply.WrongLeader = false

	op := Op{args.Key, "", GET, args.Id, args.ReqId}

	applyReply := kv.startAgree(op)

	reply.Value = applyReply.value
	reply.Err = Err(applyReply.err)

	raft.Log("server.go: server %d Get over, {key: %s, value: %s}, isLeader: %t, clientId: %d, err: %s\n",
		kv.me, args.Key, reply.Value, isLeader, args.Id, reply.Err)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.Log("server.go: server %d PutAppend, {key: %s, value: %s, op: %s}, clientId: %d\n",
		kv.me, args.Key, args.Value, args.Op, args.Id)
	reply.WrongLeader = false

	op := Op{args.Key, args.Value, args.Op, args.Id, args.ReqId}

	applyReply := kv.startAgree(op)

	reply.Err = Err(applyReply.err)

	raft.Log("server.go: server %d PutAppend over, {key: %s, value: %s, op: %s}, leader: %t, clientId: %d, err: %s \n",
		kv.me, args.Key, kv.store[op.Key], args.Op, isLeader, args.Id, reply.Err)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	raft.Log("server.go: server %d killed\n", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	raft.Log2("server.go: server %d, max_raft_state: %d\n", kv.me, kv.maxraftstate)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.opChans = make(map[int]chan ApplyReply)
	kv.store = make(map[string]string)
	kv.executed = make(map[int64]int64)

	go waitToAgree(kv)

	return kv
}

func waitToAgree(kv *RaftKV) {

	for {
		applyMsg := <- kv.applyCh

		if applyMsg.UseSnapshot {
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := gob.NewDecoder(r)

			kv.mu.Lock()
			kv.store = make(map[string]string)
			kv.executed = make(map[int64]int64)
			d.Decode(&kv.executed)
			d.Decode(&kv.store)
			kv.mu.Unlock()

		} else {

			op := applyMsg.Command.(Op)

			_, isLeader := kv.rf.GetState()

			raft.Log("server.go: server %d waitToAgree , op: {key: %s, value: %s, op: %s}, "+
				"isLeader: %t, clientId: %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, isLeader, op.ClientId, op.ReqId)

			reply := kv.apply(op)

			if isLeader {
				kv.mu.Lock()
				op, ok := kv.opChans[applyMsg.Index]
				kv.mu.Unlock()
				if ok {
					op <- reply
				}
			}

			kv.mu.Lock()
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate  {
				raft.Log2("server.go: server %d, max_raft_state: %d, cur_raft_state: %d \n", kv.me, kv.maxraftstate, kv.rf.GetRaftStateSize())
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.executed)
				e.Encode(kv.store)
				data := w.Bytes()
				go kv.rf.SnapShot(data, applyMsg.Index)
			}
			kv.mu.Unlock()
			raft.Log("server.go: server %d waitToAgree over, op: {key: %s, value: %s, op: %s}, "+
				"isLeader: %t, clientId: %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, isLeader, op.ClientId, op.ReqId)
		}
	}

}
