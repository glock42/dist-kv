package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu             sync.Mutex
	possibleLeader int
	reqId          int64
	id             int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.reqId = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	reqId := ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	i := ck.possibleLeader
	for {
		args := QueryArgs{}
		args.Num = num
		args.Op = QUERY
		args.ReqId = reqId
		args.Id = ck.id
		var reply QueryReply
		// try each known server.

		ok := ck.servers[i].Call("ShardMaster.Query", &args, &reply)

		if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrDupReq){
			ck.possibleLeader = i
			return reply.Config
		}

		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	reqId := ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	i := ck.possibleLeader
	for {
		args := JoinArgs{}
		args.Servers = servers
		args.Op = JOIN
		args.ReqId = reqId
		args.Id = ck.id
		var reply JoinReply
		// try each known server.

		ok := ck.servers[i].Call("ShardMaster.Join", &args, &reply)

		if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrDupReq){
			ck.possibleLeader = i
			return
		}

		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	reqId := ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	i := ck.possibleLeader
	for {
		args := LeaveArgs{}
		args.GIDs =gids
		args.Op = LEAVE
		args.ReqId = reqId
		args.Id = ck.id
		var reply LeaveReply
		// try each known server.

		ok := ck.servers[i].Call("ShardMaster.Leave", &args, &reply)

		if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrDupReq){
			ck.possibleLeader = i
			return
		}

		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	ck.mu.Lock()
	reqId := ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	i := ck.possibleLeader
	for {
		args := MoveArgs{}
		args.Shard = shard
		args.GID =gid
		args.Op = MOVE
		args.ReqId = reqId
		args.Id = ck.id
		var reply MoveReply
		// try each known server.

		ok := ck.servers[i].Call("ShardMaster.Move", &args, &reply)

		if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrDupReq){
			ck.possibleLeader = i
			return
		}

		i = (i + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
