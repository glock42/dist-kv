package shardmaster

import (
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "encoding/gob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	configNum int

	executed map[int64]int64
	opChans  map[int]chan ApplyReply

}

type ApplyReply struct {
	value string
	err   string
}

type Op struct {
	// Your data here.
	Value          interface{}
	Operation string
	ClientId  int64
	ReqId     int64
}

func (sm *ShardMaster) startAgree(op Op) ApplyReply {

	//raft.Log("server.go: server %d startAgree, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)

	index, term, _ := sm.rf.Start(op)
	sm.mu.Lock()
	opChan, ok := sm.opChans[index]
	if !ok {
		sm.opChans[index] = make(chan ApplyReply, 1)
		opChan= sm.opChans[index]
	}
	sm.mu.Unlock()
	reply := ApplyReply{}
	select {
	case reply = <- opChan:
		curTerm, isLeader := sm.rf.GetState()
		if !isLeader || term != curTerm {
			reply.value = ""
			reply.err = ERROR
		}
	case <-time.After(1000 * time.Millisecond):
		reply.err = ERROR
	}
	//raft.Log("server.go: server %d startAgree over, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d, result: %s \n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (sm *ShardMaster) doJoin(servers map[int][]string) bool {
	config := Config{}
	sm.configNum += 1
	config.Num = sm.configNum
	config.Groups = make(map[int][]string)

	for key, value := range servers {
		config.Groups[key] = value
	}

	NGroup := len(config.Groups)

	if NGroup >= NShards {
		for i:= 0; i < NShards; i++  {
			config.Shards[i] = i
		}
	} else {
		interval := NShards / NGroup
		i := 0
		for ; i < interval * NGroup ; i+=interval {
			if i > NShards {
				break
			}
			for j:= i; j < j + interval ;j++ {
				config.Shards[j] = i / interval
			}
		}
	}

	return true
}

func (sm *ShardMaster) doLeave(GIDs []int) bool {
	return false
}
func (sm *ShardMaster) apply(op Op) ApplyReply {
	//raft.Log("server.go: server %d apply, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	reply := ApplyReply{}
	reply.err = OK
	reply.value = ""
	executedReqId, _ := sm.executed[op.ClientId]
	if op.ReqId > executedReqId {
		if op.Operation == JOIN {
			servers := op.Value.(map[int][]string)
			sm.doJoin(servers)
		} else if op.Operation == LEAVE {
			GIDs := op.Value.([]int)
			sm.doLeave(GIDs)
		}
		//} else if op.Operation == APPEND {
		//	v, ok := sm.store[op.Key]
		//	if ok {
		//		sm.store[op.Key] = v + op.Value
		//	} else {
		//		reply.err = ErrNoKey
		//	}
		//} else {
		//	sm.store[op.Key] = op.Value
		//}
		sm.executed[op.ClientId] = op.ReqId
	} else {
		//raft.Log("server.go: server %d waitToAggre failed, op: {key: %s, value: %s, op: %s}," +
		//	" clientId %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
		//if op.Operation == GET {
		//	reply.value = sm.store[op.Key]
		//}
		reply.err = ErrDupReq
	}
	//raft.Log("server.go: server %d apply over, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d, err: %s\n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	op := Op{args.Servers, args.Op, args.Id, args.ReqId}

	applyReply := sm.startAgree(op)

	reply.Err = Err(applyReply.err)

	//raft.Log("server.go: server %d PutAppend over, {key: %s, value: %s, op: %s}, leader: %t, clientId: %d, err: %s \n",
	//	kv.me, args.Key, kv.store[op.Key], args.Op, isLeader, args.Id, reply.Err)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	op := Op{args.GIDs, args.Op, args.Id, args.ReqId}

	applyReply := sm.startAgree(op)

	reply.Err = Err(applyReply.err)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configNum = 0

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.opChans = make(map[int]chan ApplyReply)
	sm.executed = make(map[int64]int64)

	go waitToAgree(sm)

	return sm
}



func waitToAgree(sm *ShardMaster) {

	for {
		applyMsg := <- sm.applyCh

		op := applyMsg.Command.(Op)

		_, isLeader := sm.rf.GetState()

		//raft.Log("server.go: server %d waitToAgree , op: {key: %s, value: %s, op: %s}, "+
		//	"isLeader: %t, clientId: %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, isLeader, op.ClientId, op.ReqId)

		reply := sm.apply(op)

		if isLeader {
			sm.mu.Lock()
			op, ok := sm.opChans[applyMsg.Index]
			sm.mu.Unlock()
			if ok {
				op <- reply
			}
		}

		//raft.Log("server.go: server %d waitToAgree over, op: {key: %s, value: %s, op: %s}, "+
		//	"isLeader: %t, clientId: %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, isLeader, op.ClientId, op.ReqId)
	}

}