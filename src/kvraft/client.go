package raftkv

import "labrpc"
import "crypto/rand"
import  "raft"
import (
	"math/big"
		)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	//fmt.Printf("client.go: Get, {key: %s}\n", key)


	result := ""

	for {
		for i := 0; i < len(ck.servers); i++ {
			args := GetArgs{}
			reply := GetReply{}
			args.Key = key
			raft.Log("client.go: call server Get, {key: %s}\n", key)
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)

			if ok && !reply.WrongLeader {
				result = reply.Value
				raft.Log("client.go: call server Get return, {key: %s, value: %s}, reply{wrongLeader: %t}\n",
					 key, reply.Value, reply.WrongLeader)
				return result
			}
		}
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

	raft.Log("client.go: PutAppend, {key: %s, value: %s, op: %s}\n", key, value, op)

	for {

		for i := 0; i < len(ck.servers); i++ {
			args := PutAppendArgs{}
			reply := PutAppendReply{}
			args.Key = key
			args.Value = value
			args.Op = op

			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)

			if ok && reply.WrongLeader == false {
				raft.Log("client.go: PutAppend server return, {key: %s, value: %s, op: %s}\n", key, value, op)
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
