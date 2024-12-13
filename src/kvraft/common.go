package kvraft

// const (
// 	OK             = "OK"
// 	ErrNoKey       = "ErrNoKey"
// 	ErrWrongLeader = "ErrWrongLeader"
// )

type Err string
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op         string
	ClientId   int64
	SeqNum int
}

type RequestResult struct {
	SeqNum int
	Value      string
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId   int64
	SeqNum int
}

type GetReply struct {
	Err   string
	Value string
}