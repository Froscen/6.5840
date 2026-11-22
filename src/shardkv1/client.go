package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	mu  sync.Mutex
	cfg *shardcfg.ShardConfig
}

func (ck *Clerk) cachedConfig() *shardcfg.ShardConfig {
	ck.mu.Lock()
	cfg := ck.cfg
	ck.mu.Unlock()
	if cfg != nil {
		return cfg
	}
	return ck.refreshConfig(nil)
}

func (ck *Clerk) refreshConfig(old *shardcfg.ShardConfig) *shardcfg.ShardConfig {
	for {
		cfg := ck.sck.Query()
		if cfg == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		ck.mu.Lock()
		ck.cfg = cfg
		ck.mu.Unlock()
		if old != nil && cfg.Num == old.Num {
			time.Sleep(50 * time.Millisecond)
		}
		return cfg
	}
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	shard := shardcfg.Key2Shard(key)

	config := ck.cachedConfig()
	for {
		gid := config.Shards[shard]
		if gid == 0 {
			config = ck.refreshConfig(config)
			continue
		}
		clerk := ck.GetOrCreateClerk(gid, config.Groups[gid])

		value, version, err := clerk.Get(key)
		if err == rpc.ErrWrongGroup || err == rpc.ErrWrongLeader {
			// shard moved, or group unreachable: re-query config
			config = ck.refreshConfig(config)
			continue
		}
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	shard := shardcfg.Key2Shard(key)
	maybe := false
	config := ck.cachedConfig()
	for {
		gid := config.Shards[shard]
		if gid == 0 {
			config = ck.refreshConfig(config)
			continue
		}
		clerk := ck.GetOrCreateClerk(gid, config.Groups[gid])

		err := clerk.Put(key, value, version)
		switch err {
		case rpc.ErrWrongGroup, rpc.ErrWrongLeader:
			maybe = true
			config = ck.refreshConfig(config)
			continue
		case rpc.ErrVersion:
			if maybe {
				return rpc.ErrMaybe
			}
			return err
		default:
			return err
		}
	}
}

func (ck *Clerk) GetOrCreateClerk(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	clerk, ok := ck.rcks[gid]
	if ok {
		return clerk
	}

	clerk = shardgrp.MakeClerk(ck.clnt, servers)
	ck.rcks[gid] = clerk

	return clerk
}
