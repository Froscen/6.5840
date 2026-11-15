package kvraft

import (
	"bytes"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type ClientId int

type KVEntry struct {
	Value string
	Version rpc.Tversion
}

type LastRequest struct {
	ID int
	Result any
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	data map[string]KVEntry
	requests map[ClientId]LastRequest
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch args := req.(type) {
	case rpc.GetArgs:
		last, hasLastRequest := kv.requests[ClientId(args.ClientId)]
		if hasLastRequest {
			if args.RequestId == last.ID {
				return last.Result
			} else if args.RequestId < last.ID {
				return rpc.GetReply{
					Err: rpc.ErrWrongLeader,
				}
			}
		}
		entry, exists := kv.data[args.Key]

		var reply rpc.GetReply
		if !exists {
			reply = rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		} else {
			reply = rpc.GetReply{
				Err: rpc.OK,
				Value: entry.Value,
				Version: entry.Version,
			}
		}

		kv.requests[ClientId(args.ClientId)] = LastRequest{
			ID: args.RequestId,
			Result: reply,
		}
		return reply
	case rpc.PutArgs:
		last, hasLastRequest := kv.requests[ClientId(args.ClientId)]
		if hasLastRequest {
			if args.RequestId == last.ID {
				return last.Result
			} else if args.RequestId < last.ID {
				return rpc.PutReply{
					Err: rpc.ErrWrongLeader,
				}
			}
		}
		entry, exists := kv.data[args.Key]

		var reply rpc.PutReply
		if !exists {
			if args.Version == 0 {
				kv.data[args.Key] = KVEntry{
					Value: args.Value,
					Version: 1,
				}

				reply = rpc.PutReply{
					Err: rpc.OK,
				}
			} else {
				reply = rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			}
		} else {
			if entry.Version == args.Version {
				kv.data[args.Key] = KVEntry{
					Value: args.Value,
					Version: entry.Version + 1,
				}

				reply = rpc.PutReply{
					Err: rpc.OK,
				}
			} else {
				reply = rpc.PutReply{
					Err: rpc.ErrVersion,
				}
			}
		}

		kv.requests[ClientId(args.ClientId)] = LastRequest{
			ID: args.RequestId,
			Result: reply,
		}

		return reply
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.data); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.requests); err != nil {
		panic(err)
	}

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kdata map[string]KVEntry
	var requests map[ClientId]LastRequest

	if err := d.Decode(&kdata); err != nil {
		panic(err)
	}
	if err := d.Decode(&requests); err != nil {
		panic(err)
	}

	kv.data = kdata
	kv.requests = requests
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.OK {
		*reply = res.(rpc.GetReply)
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)
	if err == rpc.OK {
		*reply = res.(rpc.PutReply)
	} else {
		reply.Err = err
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{
		me: me,
		data: make(map[string]KVEntry),
		requests: make(map[ClientId]LastRequest),
	}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, grp, srv, persister, tester.MaxRaftState)
}
