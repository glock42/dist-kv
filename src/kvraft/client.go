package raftkv

import "labrpc"
import "crypto/rand"
import  "raft"
import (
	"math/big"
	"sync"
	)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	possibleLeader int
	reqId          int64
	id             int64
	mu             sync.Mutex
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
	ck.id = nrand()
	ck.reqId = 1
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {


	result := ""
	ck.mu.Lock()
	reqId := ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	raft.Log("client.go: Get, {key: %s}, id: %d, reqId: %d\n", key, ck.id, reqId)
	defer raft.Log("client.go: Get over, {key: %s}, id: %d, reqId: %d\n", key, ck.id, reqId)

	i := ck.possibleLeader
	for {
		args := GetArgs{}
		reply := GetReply{}
		args.Key = key
		args.ReqId = reqId
		args.Id  = ck.id
		//raft.Log("client.go: call server Get, {key: %s}\n", key)
		ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)

		if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrDupReq){
			result = reply.Value
			raft.Log("client.go: call server Get return, {key: %s, value: %s}, reply{wrongLeader: %t}\n",
				 key, reply.Value, reply.WrongLeader)
			ck.possibleLeader = i
			return result
		}
		i = (i + 1) % len(ck.servers)
	}
	// You will have to modify this function.
	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.


	ck.mu.Lock()
	reqId := ck.reqId
	ck.reqId += 1
	ck.mu.Unlock()

	raft.Log("client.go: PutAppend, {key: %s, value: %s}, id: %d, reqId: %d\n", key, value, ck.id , reqId)
	defer raft.Log("client.go: PutAppend over, {key: %s, value: %s}, id: %d, reqId: %d\n", key, value, ck.id, reqId)

	i := ck.possibleLeader
	for {
		args := PutAppendArgs{}
		reply := PutAppendReply{}
		args.Key = key
		args.Value = value
		args.Op = op
		args.ReqId = reqId
		args.Id = ck.id
		//raft.Log("client.go: PutAppend, {key: %s, value: %s, op: %s}\n",  key, value, op)

		ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)

		if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrDupReq) {
			raft.Log("client.go: PutAppend server return, {key: %s, value: %s, op: %s}\n", key, value, op)
			ck.possibleLeader = i
			return
		}
		i = (i + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
