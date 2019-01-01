package shardkv

// import "shardmaster"
import (
	"bytes"
	"labrpc"
	"shardmaster"
	"strconv"
	"time"
)
import "raft"
import "sync"
import "encoding/gob"

const (
	GET    = "GET"
	PUT    = "Put"
	APPEND = "Append"
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
	ConfigNum int
}

type ApplyReply struct {
	value string
	err   string
}

type MigrationArg struct {
	Shard     int
	ConfigNum int
}

type MigrationData struct {
	Store       map[string]string
	Executed    map[int64]int64
}

type MigrationReply struct {
	Data      MigrationData
	Shard     int
	ConfigNum int
	Err       Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store       map[string]string
	executed    map[int64]int64
	opChans     map[int]chan ApplyReply
	shardMaster *shardmaster.Clerk
	config      shardmaster.Config

	ownShards            map[int]bool
	needToPullShards     map[int]int
	needToDispatchShards map[int]map[int]MigrationData
	configLog            map[int]shardmaster.Config
}

func (kv *ShardKV) startAgree(configNum int, op interface{}) ApplyReply {

	//raft.Log("server.go: server %d startAgree, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)

	kv.mu.Lock()
	if configNum != kv.config.Num {
		kv.mu.Unlock()
		return ApplyReply{value:"", err:ErrWrongGroup}
	}

	index, term, _ := kv.rf.Start(op)
	opChan, ok := kv.opChans[index]
	if !ok {
		kv.opChans[index] = make(chan ApplyReply, 1)
		opChan = kv.opChans[index]
	}
	//raft.Log3("server.go, server: %d, gid: %d, startAgree, wait index: %d", kv.me, kv.gid, index)
	kv.mu.Unlock()
	reply := ApplyReply{}
	select {
	case reply = <-opChan:
		curTerm, _ := kv.rf.GetState()
		if term != curTerm {
			reply.value = ""
			reply.err = ERROR
			//raft.Log3("server.go: server: %d, gid: %d, startAgree over, ERROR: different leader",
			//	kv.me, kv.gid)
		} else {
			//raft.Log3("server.go: server: %d, gid: %d, startAgree over, index: %d",
			//	kv.me, kv.gid, index)
		}
	case <-time.After(time.Duration(3 * time.Second)):
		//raft.Log3("server.go: server: %d, gid: %d, startAgree over, index: %d, ERROR: wait time out",
		//	kv.me, kv.gid, index)
		kv.mu.Lock()
		delete(kv.opChans, index)
		reply.err = ERROR
		kv.mu.Unlock()
	}
	//raft.Log("server.go: server %d startAgree over, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d, result: %s \n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (kv *ShardKV) apply(op Op) ApplyReply {
	//raft.Log3("server.go: server %d apply, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
	reply := ApplyReply{}
	reply.err = OK
	reply.value = ""
	executedReqId, _ := kv.executed[op.ClientId]
	if op.ReqId > executedReqId {
		shard := key2shard(op.Key)
		if _, ok := kv.ownShards[shard]; ok && op.ConfigNum == kv.config.Num {
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
			} else if op.Operation == PUT {
				kv.store[op.Key] = op.Value
			}
			kv.executed[op.ClientId] = op.ReqId
			raft.Log3("server.go, server: %d, gid: %d, apply %s, " +
				"{key: %s, value: %s, reply: %s, clientId: %d, reqID:%d, err: %s}, key2shard: %d, owned shards: %s\n",
				kv.me, kv.gid, op.Operation, op.Key, op.Value, kv.store[op.Key],
				op.ClientId, op.ReqId, reply.err, shard, printShards(kv.ownShards))
		} else {
			raft.Log3("server.go, server: %d, gid: %d, apply %s, want shard: %d, owned shards: %s, ErrWrongGroup\n",
				kv.me, kv.gid, op.Operation, shard, printShards(kv.ownShards))
			reply.err = ErrWrongGroup
		}
	} else {
		//raft.Log("server.go: server %d waitToAggre failed, op: {key: %s, value: %s, op: %s}," +
		//	" clientId %d, reqId: %d\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId)
		if op.Operation == GET {
			reply.value = kv.store[op.Key]
		}
		reply.err = ErrDupReq
	}
	//raft.Log3("server.go: server %d apply over, op: {key: %s, value: %s, op: %s}," +
	//	" clientId %d, reqId: %d, err: %s\n", kv.me, op.Key, op.Value, op.Operation, op.ClientId, op.ReqId, reply.err)
	return reply
}

func (kv *ShardKV) checkConfigNum(num int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != num {
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	if !kv.checkConfigNum(args.ConfigNum) {
		reply.Err = ErrWrongGroup
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	//raft.Log3("server.go: server %d Get, {key: %s}, isLeader: %t, clientId: %d\n",
	//	kv.me, args.Key, isLeader, args.Id)

	reply.WrongLeader = false

	op := Op{args.Key, "", GET, args.Id, args.ReqId, args.ConfigNum}

	applyReply := kv.startAgree(args.ConfigNum, op)

	reply.Value = applyReply.value
	reply.Err = Err(applyReply.err)

	//raft.Log3("server.go: server %d Get over, {key: %s, value: %s}, isLeader: %t, clientId: %d, err: %s\n",
	//	kv.me, args.Key, reply.Value, isLeader, args.Id, reply.Err)

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	if !kv.checkConfigNum(args.ConfigNum) {
		reply.Err = ErrWrongGroup
		return
	}

	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	raft.Log3("server.go: server: %d, gid: %d, PutAppend, {key: %s, value: %s, op: %s}, clientId: %d, reqId: %d\n",
		kv.me, kv.gid, args.Key, args.Value, args.Op, args.Id, args.ReqId)
	reply.WrongLeader = false

	op := Op{args.Key, args.Value, args.Op, args.Id, args.ReqId, args.ConfigNum}

	applyReply := kv.startAgree(args.ConfigNum, op)

	reply.Err = Err(applyReply.err)

	//raft.Log3("server.go: server: %d, gid: %d, PutAppend over, "+
	//	"{key: %s, value: %s, op: %s}, leader: %t, clientId: %d, err: %s \n",
	//	kv.me, kv.gid, args.Key, kv.store[op.Key], args.Op, isLeader, args.Id, reply.Err)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	raft.Log3("kill shardKV server: %d, gid: %d\n", kv.me, kv.gid)
}

func (kv *ShardKV) Migration(arg *MigrationArg, reply *MigrationReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if arg.ConfigNum >= kv.config.Num || len(kv.needToDispatchShards) <= 0 {
		raft.Log3("server.go: server: %d, gid: %d, Migration ERROR, arg.ConfigNum:%d, kv.ConfigNUm: %d, " +
			"len(shards): %d, shard: %d\n",
			kv.me, kv.gid, arg.ConfigNum, kv.config.Num, len(kv.needToDispatchShards), arg.Shard)
		reply.Err = ErrWrongGroup
		return
	}

	raft.Log3("server.go: server: %d, gid: %d, Migration OK, shard: %d\n", kv.me, kv.gid, arg.Shard)

	reply.Err = OK
	reply.Shard = arg.Shard
	reply.ConfigNum = arg.ConfigNum
	reply.Data = MigrationData{}
	reply.Data.Store = make(map[string]string)
	reply.Data.Executed = make(map[int64]int64)

	for k, v := range kv.needToDispatchShards[arg.ConfigNum][arg.Shard].Store {
		reply.Data.Store[k] = v
	}

	for k, v := range kv.needToDispatchShards[arg.ConfigNum][arg.Shard].Executed{
		reply.Data.Executed[k] = v
	}

}

func (kv *ShardKV) doPoll() {
	kv.mu.Lock()

	_, isLeader := kv.rf.GetState()
	if !isLeader || len(kv.needToPullShards) > 0 {
		kv.mu.Unlock()
		return
	}

	newConfigNum := kv.config.Num + 1

	kv.mu.Unlock()

	newConfig := kv.shardMaster.Query(newConfigNum)
	if newConfig.Num == newConfigNum {
		raft.Log3("server.go: server: %d, gid: %d, doPoll, poll the newConfig, num: %d\n", kv.me, kv.gid, newConfig.Num)
		kv.rf.Start(newConfig)
	}
}

func printShards(shards map[int]bool) string {
	s := ""
	for k := range shards {
		s += strconv.Itoa(k) + ", "
	}

	if len(s) > 0 {
		s = s[0 : len(s)-2]
	}

	return s
}

func printConfigShards(shards [10]int) string {
	s := ""
	for _, k := range shards {
		s += strconv.Itoa(k) + ", "
	}

	if len(s) > 0 {
		s = s[0 : len(s)-2]
	}

	return s
}
func (kv *ShardKV) applyNewConfig(newConfig *shardmaster.Config) {


	if newConfig.Num <= kv.config.Num {
		return
	}

	raft.Log3("server.go: server: %d, gid: %d, applyNewConfig, config_shards: %s, num: %d\n",
		kv.me, kv.gid, printConfigShards(newConfig.Shards), newConfig.Num)

	oldOwnShards := kv.ownShards

	kv.ownShards = make(map[int]bool)

	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			if _, ok := oldOwnShards[shard]; ok || kv.config.Num == 0 {
				kv.ownShards[shard] = true
				delete(oldOwnShards, shard)
			} else {
				kv.needToPullShards[shard] = kv.config.Num
			}
		}

	}

	if len(oldOwnShards) > 0 {
		shard2Data := make(map[int]MigrationData)
		for shard := range oldOwnShards {
			data := MigrationData{}
			data.Store = make(map[string]string)
			data.Executed = make(map[int64]int64)
			for k, v := range kv.store {
				if key2shard(k) == shard {
					data.Store[k] = v
					delete(kv.store, k)
				}
			}

			for k, v := range kv.executed {
					data.Executed[k] = v
			}

			shard2Data[shard] = data
		}
		kv.needToDispatchShards[kv.config.Num] = shard2Data
	}

	kv.config = newConfig.Copy()
	kv.configLog[kv.config.Num] = kv.config.Copy()
	if len(kv.needToPullShards) > 0 {
		raft.Log3("server.go: server: %d, gid: %d, "+
			"applyNewConfig over, num: %d, owned shards: %s, but need to pull \n",
			kv.me, kv.gid, newConfig.Num, printShards(kv.ownShards))
	} else {
		raft.Log3("server.go: server: %d, gid: %d, applyNewConfig over, num: %d, owned shards: %s \n",
			kv.me, kv.gid, newConfig.Num, printShards(kv.ownShards))
	}

}

func (kv *ShardKV) doPull() {
	kv.mu.Lock()

	_, isLeader := kv.rf.GetState()

	if !isLeader ||  len(kv.needToPullShards) <= 0{
		kv.mu.Unlock()
		return
	}

	raft.Log3("server.go: server: %d, gid: %d, numOfShardsNeedToPull: %d doPull\n", kv.me, kv.gid, len(kv.needToPullShards))
	defer raft.Log3("server.go: server: %d, gid: %d, numOfShardsToPull: %d, doPull over\n", kv.me, kv.gid, len(kv.needToPullShards))

	ch := make(chan int)
	count := len(kv.needToPullShards)

	for shard, configNum := range kv.needToPullShards {
		migrationArg := MigrationArg{}
		migrationArg.Shard = shard
		migrationArg.ConfigNum = configNum

		gid := kv.configLog[configNum].Shards[shard]

		go func(cfg shardmaster.Config) {
			defer func() { ch <- 0 }()
			if servers, ok := cfg.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {
					srv := kv.make_end(servers[si])

					var reply MigrationReply

					//raft.Log3("server.go: server: %d, gid: %d, doPull, call server %s to migration %d \n",
					//	kv.me, kv.gid, servers[si], shard)

					ok := srv.Call("ShardKV.Migration", &migrationArg, &reply)

					if ok && reply.Err == OK {
						//raft.Log3("server.go: server: %d, gid: %d, doPull OK, call server %s to migration %d back, " +
						//	"startAgreeReply\n", kv.me, kv.gid, servers[si], shard)
						kv.startAgree(kv.config.Num, reply)
						break
					} else {
						//raft.Log3("server.go: server: %d, gid: %d, doPull failed, call server %s migration %d back\n",
						//	kv.me, kv.gid, servers[si], shard)
					}
				}
			}
		}(kv.configLog[configNum].Copy())
	}

	kv.mu.Unlock()
	for count > 0 {
		<-ch
		count--
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(MigrationData{})
	gob.Register(MigrationArg{})
	gob.Register(MigrationReply{})
	gob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.shardMaster = shardmaster.MakeClerk(masters)
	kv.config = shardmaster.Config{}

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.opChans = make(map[int]chan ApplyReply)
	kv.store = make(map[string]string)
	kv.executed = make(map[int64]int64)

	kv.ownShards = make(map[int]bool)
	kv.needToPullShards = make(map[int]int)
	kv.needToDispatchShards = make(map[int]map[int]MigrationData)
	kv.configLog = make(map[int]shardmaster.Config)
	kv.configLog[kv.config.Num] = kv.config.Copy()

	raft.Log3("start shardKV server: %d, gid: %d \n", kv.me, gid)
	go waitToAgree(kv)
	go poll(kv)
	go pull(kv)

	return kv
}

func pull(kv *ShardKV) {
	for {
		select {
		case <-time.After(150 * time.Millisecond):
			kv.doPull()
		}
	}
}

func poll(kv *ShardKV) {
	for {
		select {
		case <-time.After(250 * time.Millisecond):
			kv.doPoll()
		}
	}
}

func (kv *ShardKV) notify(reply ApplyReply, index int) {
	op, ok := kv.opChans[index]
	if ok {
		op <- reply
		delete(kv.opChans, index)
	}
}

func waitToAgree(kv *ShardKV) {

	for {
		applyMsg := <-kv.applyCh

		kv.mu.Lock()
		if applyMsg.UseSnapshot {
			raft.Log3("server.go, server: %d, gid: %d, snapshot\n", kv.me, kv.gid)
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := gob.NewDecoder(r)

			kv.store = make(map[string]string)
			kv.executed = make(map[int64]int64)

			kv.ownShards = make(map[int]bool)
			kv.needToPullShards = make(map[int]int)
			kv.needToDispatchShards = make(map[int]map[int]MigrationData)
			kv.configLog = make(map[int]shardmaster.Config)

			d.Decode(&kv.executed)
			d.Decode(&kv.store)

			d.Decode(&kv.ownShards)
			d.Decode(&kv.needToPullShards)
			d.Decode(&kv.needToDispatchShards)
			d.Decode(&kv.configLog)
			d.Decode(&kv.config)

		} else {

			op, ok := applyMsg.Command.(Op)

			//_, isLeader := kv.rf.GetState()
			if ok {

				//raft.Log3("server.go: server: %d, gid: %d, waitToAgree , op: {key: %s, value: %s, op: %s}, "+
				//	"isLeader: %t, clientId: %d, reqId: %d\n", kv.me, kv.gid, op.Key, op.Value, op.Operation, isLeader, op.ClientId, op.ReqId)

				reply := kv.apply(op)

				//raft.Log3("server.go: server: %d, gid: %d, waitToAgree, notify index: %d\n",
				//	kv.me, kv.gid, applyMsg.Index)
				kv.notify(reply, applyMsg.Index)


				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					raft.Log3("server.go: server %d, gid: %d, max_raft_state: %d, cur_raft_state: %d \n",
						kv.me, kv.gid, kv.maxraftstate, kv.rf.GetRaftStateSize())
					w := new(bytes.Buffer)
					e := gob.NewEncoder(w)
					e.Encode(kv.executed)
					e.Encode(kv.store)

					e.Encode(kv.ownShards)
					e.Encode(kv.needToPullShards)
					e.Encode(kv.needToDispatchShards)
					e.Encode(kv.configLog)
					e.Encode(kv.config)
					data := w.Bytes()
					go kv.rf.SnapShot(data, applyMsg.Index)
				}
				//raft.Log3("server.go: server: %d, gid: %d, waitToAgree over, op: {key: %s, value: %s, op: %s}, "+
				//	"isLeader: %t, clientId: %d, reqId: %d\n", kv.me, kv.gid, op.Key, op.Value, op.Operation, isLeader, op.ClientId, op.ReqId)
			} else if reply, ok := applyMsg.Command.(MigrationReply); ok {
				if reply.ConfigNum == kv.config.Num - 1 {

					delete(kv.needToPullShards, reply.Shard)
					kv.ownShards[reply.Shard] = true

					for k, v := range reply.Data.Store {
						kv.store[k] = v
					}

					for k, v := range reply.Data.Executed {
						if val, ok := kv.executed[k]; ok {
							if val < v {
								kv.executed[k] = v
							}
						} else {
							kv.executed[k] = v
						}
					}
					raft.Log3("server.go: server: %d, gid: %d, migrationReply, ownShards:%s\n",
						kv.me, kv.gid, printShards(kv.ownShards))
				}

				reply := ApplyReply{}
				reply.err = OK
				kv.notify(reply, applyMsg.Index)

			} else {
				newConfig := applyMsg.Command.(shardmaster.Config)
				raft.Log3("server.go: server: %d, gid: %d, get the new config, num: %d\n",
					kv.me, kv.gid, newConfig.Num)
				kv.applyNewConfig(&newConfig)
			}
		}
		kv.mu.Unlock()
	}

}
