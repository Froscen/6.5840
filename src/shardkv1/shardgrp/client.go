package shardgrp

import (
	"time"
	

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"

	//"6.5840/shardkv1/shardgrp"

	//"6.5840/shardkv1/shardgrp"

	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{
		Key: key,
	}

	n := len(ck.servers)
	if n == 0 {
		return "", 0, rpc.ErrMaybe
	}

	for round := 0; round < maxRounds; round++ {
		for i := 0; i < n; i++ {
			server := (ck.leader + i) % n
			var reply rpc.GetReply

			ok := ck.Clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
			if !ok {
				continue
			}

			if reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrMaybe {
				continue
			}
			ck.leader = server
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", 0, rpc.ErrWrongLeader
}

const maxRounds = 10

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	n := len(ck.servers)
	if n == 0 {
		return rpc.ErrMaybe
	}

	resent := false
	for round := 0; round < maxRounds; round++ {
		for i := 0; i < n; i++ {
			server := (ck.leader + i) % n
			var reply rpc.PutReply

			ok := ck.Clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrMaybe {
				resent = true
				continue
			}
			ck.leader = server
			if reply.Err == rpc.ErrVersion && resent {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, bool, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	for try := 0; try < maxRounds*len(ck.servers); try++ {
		var reply shardrpc.FreezeShardReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.OK {
			return reply.State, reply.Moved, rpc.OK
		}
		return nil, false, reply.Err
	}

	return nil, false, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	for try := 0; try < maxRounds*len(ck.servers); try++ {
		var reply shardrpc.InstallShardReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.OK {
			return rpc.OK
		}
		return reply.Err
	}
	
	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	for try := 0; try < maxRounds*len(ck.servers); try++ {
		var reply shardrpc.DeleteShardReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if reply.Err == rpc.OK {
			return rpc.OK
		}
		return reply.Err
	}
	
	return rpc.ErrWrongLeader
}

func (ck *Clerk) Config(config shardcfg.ShardConfig) rpc.Err {
	if len(ck.servers) == 0 {
		return rpc.ErrMaybe
	}

	args := shardrpc.ConfigArgs{
		Config: config,
	}

	for try := 0; try < maxRounds*len(ck.servers); try++ {
		var reply shardrpc.ConfigReply
		ok := ck.Clnt.Call(ck.servers[ck.leader], "KVServer.Config", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrMaybe {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return reply.Err
	}
	
	return rpc.ErrWrongLeader
}
