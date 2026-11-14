package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	lockname string
	identity string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck: ck,
		lockname: lockname,
		identity: kvtest.RandValue(12),
	}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockname)
	
		if err == rpc.ErrNoKey {
			putErr := lk.ck.Put(lk.lockname, lk.identity, 0)
			if putErr == rpc.OK {
				return
			}
			if putErr == rpc.ErrMaybe {
				newValue, _, getErr := lk.ck.Get(lk.lockname)
				if newValue == lk.identity && getErr == rpc.OK {
					return
				}
				continue
			}
			continue
		}

		if err != rpc.OK {
			continue
		}

		if value != "" {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		putErr := lk.ck.Put(lk.lockname, lk.identity, version)
		if putErr == rpc.OK {
			return
		}
		if putErr == rpc.ErrMaybe {
			newValue, _, getErr := lk.ck.Get(lk.lockname)
			if newValue == lk.identity && getErr == rpc.OK {
				return
			}
			continue
		}
		
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK || value != lk.identity {
			return
		}

		putErr := lk.ck.Put(lk.lockname, "", version)
		if putErr == rpc.ErrVersion {
			continue
		} 
		if putErr == rpc.ErrMaybe {
			value, _, getErr := lk.ck.Get(lk.lockname)
			
			if value == "" && getErr == rpc.OK{
				return
			}
			if getErr == rpc.ErrNoKey {
				return
			}
			if getErr == rpc.OK && value == lk.identity {
				continue
			}
			return
		}
	}
}
