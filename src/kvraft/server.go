package raftkv

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	)

const (
	PUT = "PUT"
	APPEND = "APPEND"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Operation string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store map[string]string
	putChan chan bool
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	raft.Log("server.go: server %d Get, {key: %s}, isLeader: %t \n", kv.me, args.Key, isLeader)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	value, ok := kv.store[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}


}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.Log("server.go: server %d PutAppend, {key: %s, value: %s, op: %s} \n",
		kv.me, args.Key, args.Value, args.Op)
	reply.WrongLeader = false

	kv.rf.Start(Op{args.Key, args.Value, args.Op})
	<- kv.putChan
	raft.Log("server.go: server %d PutAppend over, {key: %s, value: %s, op: %s}, leader: %t \n",
		kv.me, args.Key, args.Value, args.Op, isLeader)
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.putChan = make(chan bool, 1)
	kv.store = make(map[string]string)

	go waitToApply(kv)

	return kv
}

func waitToApply(kv *RaftKV) {

	for {
		applyMsg := <- kv.applyCh
		op := applyMsg.Command.(Op)
		if op.Operation == APPEND {
			value, _ := kv.store[op.Key]
			kv.store[op.Key] = value + op.Value
		} else {
			kv.store[op.Key] = op.Value
		}
		_, isLeader := kv.rf.GetState()
		raft.Log("server.go: server %d waitToApply, apply op: {key: %s, value: %s}, isLeader: %t \n",
			kv.me, op.Key, op.Value, isLeader)
		if isLeader {
			kv.putChan <- true
		}
		raft.Log("server.go: server %d waitToApply over, apply op: {key: %s, value: %s}, isLeader: %t \n",
			kv.me, op.Key, op.Value, isLeader)
	}

}
