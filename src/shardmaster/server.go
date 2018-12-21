package shardmaster

import (
	"raft"
	"strconv"
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

	executed map[int64]int64
	opChans  map[int]chan ApplyReply
}

type ApplyReply struct {
	result interface{}
	err   string
}

type Op struct {
	Operation string
	ClientId  int64
	ReqId     int64

	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int
}

func (sm *ShardMaster) startAgree(op Op) ApplyReply {

	//raft.Log("server.go: server %d startAgree, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)

	index, term, _ := sm.rf.Start(op)
	sm.mu.Lock()
	opChan, ok := sm.opChans[index]
	if !ok {
		sm.opChans[index] = make(chan ApplyReply, 1)
		opChan = sm.opChans[index]
	}
	sm.mu.Unlock()
	reply := ApplyReply{}
	select {
	case reply = <-opChan:
		curTerm, isLeader := sm.rf.GetState()
		if !isLeader || term != curTerm {
			reply.result = ""
			reply.err = ERROR
		}
	case <-time.After(1000 * time.Millisecond):
		reply.err = ERROR
	}
	//raft.Log("server.go: server %d startAgree over, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d, result: %s \n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (sm *ShardMaster) copyConfig(source *Config, dest *Config) {

	dest.Num = source.Num
	dest.Shards = source.Shards
	dest.Groups = make(map[int][]string)
	for k, v := range source.Groups {
		var servers = make([]string, len(v))
		copy(servers, v)
		dest.Groups[k] = servers
	}
}

func (sm *ShardMaster) reBalance(config *Config, joinGids map[int]bool, leaveGids map[int]bool) {
	NGroup := len(config.Groups)
	avg := NShards / NGroup

	gid2ShardsNum := make(map[int]int)
	lessThanAvg := make([]int, 0)
	moreThanAvg := make([]int, 0)
	equalAvg := make([]int, 0)

	for _, gid := range config.Shards {
		if gid == 0 {
			continue
		}
		gid2ShardsNum[gid] += 1
	}

	for gid := range joinGids {
		gid2ShardsNum[gid] = 0
	}

	for gid, num := range gid2ShardsNum {
		if num < avg {
			lessThanAvg = append(lessThanAvg, gid)
		} else if num > avg {
			moreThanAvg = append(moreThanAvg, gid)
		} else {
			equalAvg = append(equalAvg, gid)
		}
	}


	for i, gid := range config.Shards {
		num, _ := gid2ShardsNum[gid]

		if _, ok := leaveGids[gid]; ok || num > avg {
			if len(lessThanAvg) > 0 {
				lessThanAvgGid := lessThanAvg[0]
				config.Shards[i] = lessThanAvgGid
				gid2ShardsNum[gid] -= 1
				gid2ShardsNum[lessThanAvgGid] += 1
				if gid2ShardsNum[lessThanAvgGid] >= avg {
					lessThanAvg = lessThanAvg[1:]
				}
			}
		} else if num < avg {
			if len(moreThanAvg) > 0 {
				moreThanAvgGid := moreThanAvg[0]
				config.Shards[i] = gid
				gid2ShardsNum[moreThanAvgGid] -= 1
				gid2ShardsNum[gid] +=1
				if gid2ShardsNum[moreThanAvgGid] <= avg {
					moreThanAvg = moreThanAvg[1:]
				}
			}
		}
	}
}

func (sm *ShardMaster) joinBalance(config *Config, joinGids []int) {
	NGroup := len(config.Groups)
	avg := NShards / NGroup

	gid2Shards := make(map[int][]int)
	joinGid2ShardsNum := make(map[int]int)
	moreThanAvg := make(map[int][]int)

	for i, gid := range config.Shards {
		gid2Shards[gid] = append(gid2Shards[gid], i)
	}

	for gid, shards := range gid2Shards {
		if gid == 0 || len(shards) > avg {
			moreThanAvg[gid] = shards
		}
	}

	i := 0
	dispatch := true
	for gid, shards := range moreThanAvg {
		split := avg
		if gid == 0 {
			split = 0
		}
		for shard := range shards[split: ]{
			config.Shards[shard] = joinGids[i]
			joinGid2ShardsNum[joinGids[i]] += 1
			if joinGid2ShardsNum[joinGids[i]] >= avg  {
				if i+1 >= len(joinGids) {
					if gid != 0 {
						dispatch = false
						break
					}
				} else  {
					i++
				}
			}
		}
		if !dispatch {
			break
		}
	}
}

func (sm *ShardMaster) leaveBalance(config *Config, leaveGids []int) {
	NGroup := len(config.Groups)
	if NGroup == 0 {
		for i := 0; i < len(config.Shards); i++{
			config.Shards[i] = 0
		}
		return
	}
	avg := NShards / NGroup

	gid2Shards := make(map[int][]int)
	lessThanAvg := make([]int, 0)

	for i, gid := range config.Shards {
		gid2Shards[gid] = append(gid2Shards[gid], i)
	}

	for gid, shards := range gid2Shards {
		isLeave := false
		for _, leaveGid := range leaveGids {
			if gid == leaveGid {
				isLeave = true
				break
			}
		}

		if !isLeave && len(shards) < avg {
			lessThanAvg = append(lessThanAvg, gid)
		}
	}

	i := 0
	for _, gid := range leaveGids {
		shards := gid2Shards[gid]
		for _, shard := range shards {

			if len(lessThanAvg) == 0 {
				print("catch")
			}
			lessThanAvgGid := lessThanAvg[i]
			config.Shards[shard] = lessThanAvgGid
			gid2Shards[lessThanAvgGid] = append(gid2Shards[lessThanAvgGid], shard)
			if len(gid2Shards[lessThanAvgGid]) >= avg  && i + 1 < len(lessThanAvg){
				i ++
			}
		}
	}
}

func (sm *ShardMaster) doJoin(servers map[int][]string) bool {
	config := Config{}
	sm.copyConfig(&sm.configs[len(sm.configs)-1], &config)
	config.Num ++

	joinGids := make([]int, 0)
	for key, value := range servers {
		config.Groups[key] = value
		joinGids = append(joinGids, key)
	}

	sm.joinBalance(&config, joinGids)
	sm.configs = append(sm.configs, config)
	raft.Log2("server.go: server %d doJoin over, len(config.Groups): %d, joinGids: %s, shards: %s\n",
		sm.me, len(config.Groups), printGids(joinGids) , printShards(config.Shards))
	return true
}

func (sm *ShardMaster) doLeave(GIDs []int) bool {
	config := Config{}
	sm.copyConfig(&sm.configs[len(sm.configs)-1], &config)
	config.Num ++

	leaveGids := make([]int, 0)
	for _, gid := range GIDs {
		delete(config.Groups, gid)
		leaveGids = append(leaveGids, gid)
	}

	sm.leaveBalance(&config, leaveGids)
	sm.configs = append(sm.configs, config)
	raft.Log2("server.go: server %d doLeave over, len(config.Groups): %d, shards: %s\n",
		sm.me, len(config.Groups), printShards(config.Shards))
	return true
}

func (sm *ShardMaster) doMove(shard int, gid int) bool {
	config := Config{}
	sm.copyConfig(&sm.configs[len(sm.configs)-1], &config)
	config.Num ++

	config.Shards[shard] = gid

	sm.configs = append(sm.configs, config)
	raft.Log2("server.go: server %d doMove over, len(config.Groups): %d, shards: %s\n",
		sm.me, len(config.Groups), printShards(config.Shards))
	return true
}

func (sm *ShardMaster) doQuery(num int) Config {
	raft.Log2("server.go: server %d doQuery, num: %d\n", sm.me, num)
	latestConfig := sm.configs[len(sm.configs) - 1]
	if num == -1 || num > latestConfig.Num {
		return latestConfig
	}

	for _, config := range sm.configs  {
		if config.Num == num {
			return config
		}
	}

	return latestConfig
}


func (sm *ShardMaster) apply(op Op) ApplyReply {
	//raft.Log("server.go: server %d apply, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d\n", sm.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	reply := ApplyReply{}
	reply.err = OK
	reply.result = ""
	executedReqId, _ := sm.executed[op.ClientId]
	if op.ReqId > executedReqId {
		if op.Operation == JOIN {
			sm.doJoin(op.Servers)
		} else if op.Operation == LEAVE {
			sm.doLeave(op.GIDs)
		} else if op.Operation == MOVE {
			sm.doMove(op.Shard, op.GID)
		} else if op.Operation == QUERY {
			config:= sm.doQuery(op.Num)
			reply.result = config
		}
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

func printMap(servers map[int][]string) string {
	s := ""

	for k, v := range servers {
		s += strconv.Itoa(k) + ": { "
		sub_str := ""
		for _, server := range v {
			sub_str += server + ", "
		}

		s += sub_str + " }, "
	}

	return s
}

func printShards(shards [10]int) string {
	s := ""
	for _, gid := range shards {
		s += strconv.Itoa(gid) + ", "
	}

	return s
}
func printGids(gids []int) string {
	s := ""
	for _, gid := range gids {
		s += strconv.Itoa(gid) + ", "
	}

	return s
}
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	raft.Log2("server.go: server %d, Join, server: %s\n", sm.me, printMap(args.Servers))

	reply.WrongLeader = false

	op := Op{Operation: args.Op, ClientId: args.Id, ReqId: args.ReqId, Servers: args.Servers}

	applyReply := sm.startAgree(op)

	reply.Err = Err(applyReply.err)

	raft.Log2("server.go: server %d, Join over, server: %s, err: %s\n", sm.me, printMap(args.Servers), reply.Err)
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

	raft.Log2("server.go: server %d Leave, gid: %d\n",sm.me, args.GIDs)

	reply.WrongLeader = false

	op := Op{ Operation: args.Op, ClientId: args.Id, ReqId: args.ReqId, GIDs: args.GIDs}

	applyReply := sm.startAgree(op)

	reply.Err = Err(applyReply.err)
	raft.Log2("server.go: server %d Leave over, gid: %d, err: %s\n", sm.me, args.GIDs, reply.Err)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	raft.Log2("server.go: server %d Move, shard: %d, gid: %d\n", sm.me, args.Shard, args.GID)
	reply.WrongLeader = false

	op := Op{ Operation: args.Op, ClientId: args.Id, ReqId: args.ReqId, Shard: args.Shard, GID: args.GID}

	applyReply := sm.startAgree(op)

	reply.Err = Err(applyReply.err)
	raft.Log2("server.go: server %d Move over, shard: %d, gid: %d, err: %s\n",sm.me, args.Shard, args.GID, reply.Err)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	raft.Log2("server.go: server %d Query, Num: %d\n", sm.me, args.Num)
	reply.WrongLeader = false

	op := Op{ Operation: args.Op, ClientId: args.Id, ReqId: args.ReqId, Num: args.Num}

	applyReply := sm.startAgree(op)
	if applyReply.err == OK {
		reply.Config = applyReply.result.(Config)
	}
	reply.Err = Err(applyReply.err)
	raft.Log2("server.go: server %d Query over, Num: %d, err: %s\n", sm.me, args.Num, reply.Err)
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
		applyMsg := <-sm.applyCh

		if !applyMsg.UseSnapshot {

			op := applyMsg.Command.(Op)

			_, isLeader := sm.rf.GetState()

			raft.Log2("server.go: server %d waitToAgree , op: %s, "+
				"isLeader: %t, clientId: %d, reqId: %d\n", sm.me, op.Operation, isLeader, op.ClientId, op.ReqId)

			reply := sm.apply(op)

			if isLeader {
				sm.mu.Lock()
				op, ok := sm.opChans[applyMsg.Index]
				sm.mu.Unlock()
				if ok {
					op <- reply
				}
			}

			raft.Log2("server.go: server %d waitToAgree over, op: %s, "+
				"isLeader: %t, clientId: %d, reqId: %d\n", sm.me, op.Operation, isLeader, op.ClientId, op.ReqId)
		}
	}

}
