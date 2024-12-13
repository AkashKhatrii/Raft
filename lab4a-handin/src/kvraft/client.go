package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	Servers []*labrpc.ClientEnd
	ClientId int64
	SeqNum    int
	CurrentLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.Servers = servers
	ck.ClientId = nrand()
	ck.SeqNum = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.SeqNum ++
	args :=  GetArgs{
		Key: key,
		ClientId: ck.ClientId,
		SeqNum: ck.SeqNum,
	}

	serverIndex := ck.CurrentLeader
	var reply GetReply

	for {
		// ok := ck.server.Call("KVServer.Get", &args, &reply)
		ok := ck.Servers[serverIndex % len(ck.Servers)].Call("KVServer.Get", &args, &reply)
		// if ok {
		// 	return reply.Value
		// }
		if ok && reply.Err == "OK" {
			ck.CurrentLeader = serverIndex % len(ck.Servers)
			return reply.Value
		}
		serverIndex++ // going to next server
		time.Sleep(2 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.SeqNum ++
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.ClientId,
		SeqNum: ck.SeqNum,
	}

	// 2a code
	// var reply PutAppendReply

	// for {
	// 	ok := ck.server.Call("KVServer."+op, &args, &reply)
	// 	if ok {
	// 		return reply.Value
	// 	}
	// }

	serverIndex := ck.CurrentLeader
	for {
		reply := PutAppendReply{}

		ok := ck.Servers[serverIndex % len(ck.Servers)].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == "OK" {
			ck.CurrentLeader = serverIndex % len(ck.Servers)
			return
		}
		// go to next server
		serverIndex ++
		time.Sleep(2 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
