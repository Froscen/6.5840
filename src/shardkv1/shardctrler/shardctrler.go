package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"bytes"
	"time"
	

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/labgob"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	//"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	identity string
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	cvalue, cversion, cerr := sck.IKVClerk.Get("current")
	if cerr != rpc.OK {
		return
	}
	
	current := shardcfg.FromString(cvalue)
	if current == nil {
		return
	}

	nvalue, _, nerr := sck.IKVClerk.Get("next")
	if nerr != rpc.OK {
		return
	}
	if nvalue == "" {
		return
	}
	
	nextConfig := shardcfg.FromString(nvalue)

	if nextConfig == nil {
		return
	}

	if nextConfig.Num <= current.Num {
		return
	}

	if nextConfig.Num == current.Num + 1 {
		sck.finish(current, cversion, nextConfig)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.IKVClerk.Put("current", cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	cvalue, cversion, cerr := sck.IKVClerk.Get("current")
	if cerr != rpc.OK {
		return
	}

	current := shardcfg.FromString(cvalue)
	if current == nil {
		return
	}

	nvalue, nversion, nerr := sck.IKVClerk.Get("next")

	var nextConfig *shardcfg.ShardConfig
	var nextVersion rpc.Tversion

	if nerr == rpc.OK {
		if nvalue != "" {
			nextConfig = shardcfg.FromString(nvalue)
			if nextConfig == nil {
				return
			}
		}
		nextVersion = nversion
	} else if nerr == rpc.ErrNoKey {
		nextConfig = nil
		nextVersion = 0
	} else {
		return
	}
	
	if nextConfig != nil && nextConfig.Num > current.Num {
		if nextConfig.Num != current.Num + 1 {
			return
		}

		sck.finish(current, cversion, nextConfig)
		return
	}

	if new == nil || new.Num != current.Num + 1 {
		return
	}

	res := sck.IKVClerk.Put("next", new.String(), nextVersion)
	if res == rpc.ErrMaybe {
		value, _, err := sck.IKVClerk.Get("next")
		if err != rpc.OK || value != new.String() {
			return
		}
	} else if res != rpc.OK {
		return
	}
	
	sck.finish(current, cversion, new)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	value, _, err := sck.IKVClerk.Get("current")
	if err != rpc.OK {
		return nil
	}

	return shardcfg.FromString(value)
}

func (sck *ShardCtrler) finish(current *shardcfg.ShardConfig, cversion rpc.Tversion, nextConfig *shardcfg.ShardConfig) {
	if current == nil || nextConfig == nil {
		return
	}
	if nextConfig.Num != current.Num + 1 {
		return
	}
	
	for _, servers := range nextConfig.Groups {
		ck := shardgrp.MakeClerk(sck.clnt, servers)
		if !sck.retry(nextConfig, func() rpc.Err { return ck.Config(*current) }) {
			return
		}
	}

	for shard := 0; shard < shardcfg.NShards; shard++ {
		oldGID := current.Shards[shard]
		newGID := nextConfig.Shards[shard]

		if oldGID == newGID {
			continue
		}
		if oldGID == 0 {
			emptyData := make(map[string]shardgrp.Value)

			var buf bytes.Buffer
			enc := labgob.NewEncoder(&buf)
			if err := enc.Encode(emptyData); err != nil {
				return
			}

			servers := nextConfig.Groups[newGID]
			ck := shardgrp.MakeClerk(sck.clnt, servers)

			if !sck.retry(nextConfig, func() rpc.Err {
				return ck.InstallShard(shardcfg.Tshid(shard), buf.Bytes(), nextConfig.Num)
			}) {
				return
			}
		} else if newGID == 0 {
			ck := shardgrp.MakeClerk(sck.clnt, current.Groups[oldGID])
			if !sck.retry(nextConfig, func() rpc.Err {
				_, _, err := ck.FreezeShard(shardcfg.Tshid(shard), nextConfig.Num)
				return err
			}) {
				return
			}
			if !sck.retry(nextConfig, func() rpc.Err {
				return ck.DeleteShard(shardcfg.Tshid(shard), nextConfig.Num)
			}) {
				return
			}
		} else {
			source := shardgrp.MakeClerk(sck.clnt, current.Groups[oldGID])
			target := shardgrp.MakeClerk(sck.clnt, nextConfig.Groups[newGID])

			var state []byte
			var moved bool
			if !sck.retry(nextConfig, func() rpc.Err {
				var err rpc.Err
				state, moved, err = source.FreezeShard(shardcfg.Tshid(shard), nextConfig.Num)
				return err
			}) {
				return
			}

			if !moved {
				if !sck.retry(nextConfig, func() rpc.Err {
					return target.InstallShard(shardcfg.Tshid(shard), state, nextConfig.Num)
				}) {
					return
				}
			}

			if !sck.retry(nextConfig, func() rpc.Err {
				return source.DeleteShard(shardcfg.Tshid(shard), nextConfig.Num)
			}) {
				return
			}
		}
	}

	for _, servers := range nextConfig.Groups {
		ck := shardgrp.MakeClerk(sck.clnt, servers)
		if !sck.retry(nextConfig, func() rpc.Err { return ck.Config(*nextConfig) }) {
			return
		}
	}

	targetValue := nextConfig.String()
	err := sck.IKVClerk.Put("current", targetValue, cversion)
	if err != rpc.OK {
		value, _, getErr := sck.IKVClerk.Get("current")
		if getErr != rpc.OK || value != targetValue {
			return
		}
	}
}

func (sck *ShardCtrler) retry(next *shardcfg.ShardConfig, op func() rpc.Err) bool {
	for {
		err := op()
		if err == rpc.OK {
			return true
		}
		if err != rpc.ErrWrongLeader {
			return false
		}
		if cur := sck.Query(); cur != nil && cur.Num >= next.Num {
			return false
		}
		time.Sleep(100 * time.Millisecond)
	}
}
