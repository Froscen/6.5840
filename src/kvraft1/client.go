package kvraft

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"

	"crypto/rand"
	"math/big"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader int // last successful leader (index into servers[])
	// You can add to this struct.
	clientId int
	requestId int
	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		servers: servers,
		clientId: nrand(),
		leader: 0,
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.requestId++
	args := rpc.GetArgs{
		Key: key,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	var reply rpc.GetReply

	for  {
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if reply.Err == rpc.OK{
			return reply.Value, reply.Version, reply.Err
		}
		if reply.Err == rpc.ErrNoKey {
			return reply.Value, reply.Version, reply.Err
		}
	}
	// You will have to modify this function.
	return "", 0, ""
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.requestId++
	args := rpc.PutArgs{
		Key: key,
		Value: value,
		Version: version,
		ClientId: ck.clientId,
		RequestId: ck.requestId,
	}
	var reply rpc.PutReply
	rpcFailed := false

	for  {
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if !ok {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			rpcFailed = true
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if reply.Err == rpc.OK{
			return reply.Err
		}
		if reply.Err == rpc.ErrVersion {
			if rpcFailed {
				return rpc.ErrMaybe
			}
			return rpc.ErrVersion
		}
		return reply.Err
	}
	// You will have to modify this function.
	return ""
}

func nrand() int {
	max := big.NewInt(int64(1) << 62)
	x, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	return int(x.Int64())
}