package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

type result struct {
	Err   rpc.Err
	Reply any
}

type pendingRequest struct {
	Op   Op
	Ch   chan result
	Term int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	nextID  int64
	request map[int]pendingRequest
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		request:      make(map[int]pendingRequest),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()
	go rsm.ticker()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()

	rsm.nextID++
	op := Op{
		Me:  rsm.me,
		Id:  rsm.nextID,
		Req: req,
	}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan result, 1)
	rsm.request[index] = pendingRequest{
		Op:   op,
		Ch:   ch,
		Term: term,
	}
	rsm.mu.Unlock()

	res := <-ch

	return res.Err, res.Reply // i'm dead, try another server.
}

func (rsm *RSM) reader() {
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			rsm.mu.Lock()

			requests := make(map[int]pendingRequest)
			for index, request := range rsm.request {
				requests[index] = request
			}
			rsm.request = make(map[int]pendingRequest)
			rsm.mu.Unlock()

			for _, request := range requests {
				request.Ch <- result{
					Err: rpc.ErrWrongLeader,
				}
			}
			return
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			reply := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			request, ok := rsm.request[msg.CommandIndex]
			if ok {
				delete(rsm.request, msg.CommandIndex)
			}
			rsm.mu.Unlock()

			if ok {
				if request.Op.Me == op.Me && request.Op.Id == op.Id {
					request.Ch <- result{
						Err:   rpc.OK,
						Reply: reply,
					}
				} else {
					request.Ch <- result{
						Err: rpc.ErrWrongLeader,
					}
				}
			}

			rsm.Snapshot(msg.CommandIndex)
		}
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)

			rsm.mu.Lock()
			requests := make(map[int]pendingRequest)
			for index, request := range rsm.request {
				if index <= msg.SnapshotIndex {
					requests[index] = request
					delete(rsm.request, index)
				}
			}
			rsm.mu.Unlock()

			for _, request := range requests {
				request.Ch <- result{
					Err: rpc.ErrWrongLeader,
				}
			}
		}
	}
}

func (rsm *RSM) Snapshot(index int) {
	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
		snapshot := rsm.sm.Snapshot()
		rsm.rf.Snapshot(index, snapshot)
	}
}

func (rsm *RSM) ticker() {
	for {
		currentTerm, isLeader := rsm.rf.GetState()
		requests := make(map[int]pendingRequest)

		rsm.mu.Lock()
		for index, request := range rsm.request {
			if request.Term < currentTerm || !isLeader {
				requests[index] = request
				delete(rsm.request, index)
			}
		}
		rsm.mu.Unlock()

		for _, request := range requests {
			request.Ch <- result{
				Err: rpc.ErrWrongLeader,
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}
