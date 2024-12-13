package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

// func DPrintf(format string, a ...interface{}) (n int, err error) {
	// 	if Debug {
	// 		log.Printf(format, a...)
	// 	}
	// 	return
	// }

	
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type       string
	Key        string
	Value      string
	ClientId   int64
	SeqNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	lastAppliedIndex int
	store       map[string]string
	requests         map[int64]RequestResult
}



// func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
// 	// Your code here.
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// 	value, exists := kv.store[args.Key]
// 	if exists {
// 		reply.Value = value
// 	} else {
// 		reply.Value = ""
// 	}
// }

func (kv *KVServer) applyOperation(op Op) string {
	// Start the operation in the Raft log and get the term
	_, term, _ := kv.rf.Start(op)

	for {
		if kv.killed() {
			return "NOT OK"
		}
		currentTerm, isLeader := kv.rf.GetState()
		if !isLeader || currentTerm != term {
			return "NOT OK"
		}
		// Checking if the operation has  applied
		opApplied := func() bool {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			if result, exists := kv.requests[op.ClientId]; exists && result.SeqNum == op.SeqNum {
				return true
			}
			return false
		}()
		if opApplied {
			break
		}
		time.Sleep(2 * time.Millisecond) // played around with time, 2 seems to work better
	}
	return "OK"
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Checking leadership status
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "NOT OK"
		return
	}

	// duplicate requests
	kv.mu.Lock()
	if previousRequest, exists := kv.requests[args.ClientId]; exists {
		if args.SeqNum <= previousRequest.SeqNum {
			reply.Err = "OK"
			reply.Value = previousRequest.Value
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	op := Op{
		Type:     "Get",
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}

	reply.Err = kv.applyOperation(op)

	if reply.Err == "OK" {
		kv.mu.Lock()
		reply.Value = kv.requests[args.ClientId].Value
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = "NOT OK"
		return
	}

	kv.mu.Lock()

	if result, keyExists := kv.requests[args.ClientId]; keyExists {
		if args.SeqNum <= result.SeqNum {
			reply.Err = "OK"
			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()

	op := Op{
		Type:       args.Op,
		Key:        args.Key,
		Value:      args.Value,
		ClientId:   args.ClientId,
		SeqNum: args.SeqNum,
	}

	reply.Err = kv.applyOperation(op)
}


//code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.requests = make(map[int64]RequestResult)

	go func() {
		for !kv.killed() {
			channel := <-kv.applyCh
			kv.mu.Lock()
			if channel.CommandValid {
				appliedOp := channel.Command.(Op)
				result, exists := kv.requests[appliedOp.ClientId]

				if !exists || result.SeqNum < appliedOp.SeqNum {
					if appliedOp.Type == "Put" {
						kv.store[appliedOp.Key] = appliedOp.Value
					} else if appliedOp.Type == "Append" {
						kv.store[appliedOp.Key] += appliedOp.Value
					}
					kv.requests[appliedOp.ClientId] = RequestResult{
						SeqNum: appliedOp.SeqNum,
						Value:  kv.store[appliedOp.Key],
					}
				}
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
