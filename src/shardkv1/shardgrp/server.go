package shardgrp

import (
	"bytes"
	"sync"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type Value struct {
	Value   string
	Version rpc.Tversion
}

type ShardState int

const (
	ShardAbsent ShardState = iota
	ShardServing
	ShardFrozen
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu sync.Mutex

	data       map[string]Value
	shardState map[shardcfg.Tshid]ShardState
	configNum  shardcfg.Tnum
	frozenNum  map[shardcfg.Tshid]shardcfg.Tnum
	installNum map[shardcfg.Tshid]shardcfg.Tnum
	deleteNum  map[shardcfg.Tshid]shardcfg.Tnum
}

type SnapshotData struct {
	Data       map[string]Value
	ShardState map[shardcfg.Tshid]ShardState
	ConfigNum  shardcfg.Tnum
	FrozenNum  map[shardcfg.Tshid]shardcfg.Tnum
	InstallNum map[shardcfg.Tshid]shardcfg.Tnum
	DeleteNum  map[shardcfg.Tshid]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		shard := shardcfg.Key2Shard(args.Key)
		if kv.shardState[shard] != ShardServing {
			return rpc.GetReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		res, ok := kv.data[args.Key]
		if !ok {
			return rpc.GetReply{
				Err: rpc.ErrNoKey,
			}
		}

		reply := rpc.GetReply{
			Value:   res.Value,
			Version: res.Version,
			Err:     rpc.OK,
		}
		return reply
	case rpc.PutArgs:
		shard := shardcfg.Key2Shard(args.Key)
		if kv.shardState[shard] != ShardServing {
			return rpc.PutReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		res, ok := kv.data[args.Key]
		if !ok {
			if args.Version == 0 {
				value := Value{
					Value:   args.Value,
					Version: 1,
				}
				kv.data[args.Key] = value

				return rpc.PutReply{
					Err: rpc.OK,
				}
			}
		} else {
			if res.Version == args.Version {
				value := Value{
					Value:   args.Value,
					Version: res.Version + 1,
				}
				kv.data[args.Key] = value

				return rpc.PutReply{
					Err: rpc.OK,
				}
			}
		}
		return rpc.PutReply{
			Err: rpc.ErrVersion,
		}
	case shardrpc.FreezeShardArgs:
		if kv.shardState[args.Shard] == ShardServing {
			if args.Num != kv.configNum+1 {
				return shardrpc.FreezeShardReply{
					Num: args.Num,
					Err: rpc.ErrWrongGroup,
				}
			}
		} else if kv.shardState[args.Shard] == ShardFrozen {
			if args.Num != kv.frozenNum[args.Shard] {
				return shardrpc.FreezeShardReply{
					Num: args.Num,
					Err: rpc.ErrWrongGroup,
				}
			}
		} else if kv.shardState[args.Shard] == ShardAbsent {
			if kv.deleteNum[args.Shard] == args.Num {
				return shardrpc.FreezeShardReply{
					Num: args.Num,
					Err: rpc.OK,
					Moved: true,
				}
			}

			return shardrpc.FreezeShardReply{
				Num: args.Num,
				Err: rpc.ErrWrongGroup,
			}
		} else {
			return shardrpc.FreezeShardReply{
				Num: args.Num,
				Err: rpc.ErrWrongGroup,
			}
		}

		kv.shardState[args.Shard] = ShardFrozen
		kv.frozenNum[args.Shard] = args.Num

		data := make(map[string]Value)
		for k, v := range kv.data {
			if shardcfg.Key2Shard(k) == args.Shard {
				data[k] = v
			}
		}

		var buf bytes.Buffer
		enc := labgob.NewEncoder(&buf)
		err := enc.Encode(data)
		if err != nil {
			return shardrpc.FreezeShardReply{
				Num: args.Num,
				Err: rpc.ErrMaybe,
			}
		}
		state := buf.Bytes()
		return shardrpc.FreezeShardReply{
			State: state,
			Num:   args.Num,
			Err:   rpc.OK,
		}
	case shardrpc.InstallShardArgs:
		if kv.shardState[args.Shard] == ShardServing && kv.installNum[args.Shard] == args.Num {
			return shardrpc.InstallShardReply{
				Err: rpc.OK,
			}

		}
		if kv.shardState[args.Shard] != ShardAbsent {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}
		if args.Num != kv.configNum+1 {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		r := bytes.NewBuffer(args.State)
		d := labgob.NewDecoder(r)
		var value map[string]Value
		if err := d.Decode(&value); err != nil {
			return shardrpc.InstallShardReply{
				Err: rpc.ErrMaybe,
			}
		}

		for k := range value {
			if shardcfg.Key2Shard(k) != args.Shard {
				return shardrpc.InstallShardReply{
					Err: rpc.ErrWrongGroup,
				}
			}
		}
		for k := range kv.data {
			if shardcfg.Key2Shard(k) == args.Shard {
				delete(kv.data, k)
			}
		}
		for k, v := range value {
			kv.data[k] = v
		}
		kv.shardState[args.Shard] = ShardServing
		kv.installNum[args.Shard] = args.Num

		return shardrpc.InstallShardReply{
			Err: rpc.OK,
		}
	case shardrpc.DeleteShardArgs:
		if kv.shardState[args.Shard] == ShardAbsent && kv.deleteNum[args.Shard] == args.Num {
			return shardrpc.DeleteShardReply{
				Err: rpc.OK,
			}
		}

		if kv.shardState[args.Shard] != ShardFrozen {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}
		if kv.frozenNum[args.Shard] != args.Num {
			return shardrpc.DeleteShardReply{
				Err: rpc.ErrWrongGroup,
			}
		}

		for k := range kv.data {
			if shardcfg.Key2Shard(k) == args.Shard {
				delete(kv.data, k)
			}
		}
		kv.shardState[args.Shard] = ShardAbsent
		kv.deleteNum[args.Shard] = args.Num

		return shardrpc.DeleteShardReply{
			Err: rpc.OK,
		}
	case shardrpc.ConfigArgs:
		if args.Config.Num <= kv.configNum {
			return shardrpc.ConfigReply{
				Err: rpc.OK,
				CurrentNum: kv.configNum,
			}
		}

		kv.configNum = args.Config.Num
		return shardrpc.ConfigReply{
			Err: rpc.OK,
			CurrentNum: kv.configNum,
		}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	snapshot := SnapshotData{
		Data:       kv.data,
		ShardState: kv.shardState,
		ConfigNum:  kv.configNum,
		FrozenNum:  kv.frozenNum,
		InstallNum: kv.installNum,
		DeleteNum:  kv.deleteNum,
	}

	var buf bytes.Buffer
	e := labgob.NewEncoder(&buf)
	if err := e.Encode(snapshot); err != nil {
		log.Fatal(err)
	}

	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(data) == 0 {
		return
	}

	var snapshot SnapshotData
	buf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buf)
	if err := d.Decode(&snapshot); err != nil {
		log.Fatal(err)
	}

	kv.data = snapshot.Data
	kv.shardState = snapshot.ShardState
	kv.configNum = snapshot.ConfigNum
	kv.frozenNum = snapshot.FrozenNum
	kv.installNum = snapshot.InstallNum
	kv.deleteNum = snapshot.DeleteNum

	if kv.data == nil {
		kv.data = make(map[string]Value)
	}
	if kv.shardState == nil {
		kv.shardState = make(map[shardcfg.Tshid]ShardState)
	}
	if kv.frozenNum == nil {
		kv.frozenNum = make(map[shardcfg.Tshid]shardcfg.Tnum)
	}
	if kv.installNum == nil {
		kv.installNum = make(map[shardcfg.Tshid]shardcfg.Tnum)
	}
	if kv.deleteNum == nil {
		kv.deleteNum = make(map[shardcfg.Tshid]shardcfg.Tnum)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(shardrpc.InstallShardReply)

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(shardrpc.DeleteShardReply)
}

func (kv *KVServer) Config(args *shardrpc.ConfigArgs, reply *shardrpc.ConfigReply) {
	err, result := kv.rsm.Submit(*args)
	
	if err != rpc.OK {
		reply.Err = err
		return
	}
	configReply := result.(shardrpc.ConfigReply)
	*reply = configReply
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.ConfigArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:        gid,
		me:         me,
		data:       make(map[string]Value),
		shardState: make(map[shardcfg.Tshid]ShardState),
		frozenNum:  make(map[shardcfg.Tshid]shardcfg.Tnum),
		installNum: make(map[shardcfg.Tshid]shardcfg.Tnum),
		deleteNum:  make(map[shardcfg.Tshid]shardcfg.Tnum),
	}
	
	if gid == shardcfg.Gid1 {
		for shard := 0; shard < shardcfg.NShards; shard++ {
			kv.shardState[shardcfg.Tshid(shard)] = ShardServing
		}
		kv.configNum = shardcfg.NumFirst
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
