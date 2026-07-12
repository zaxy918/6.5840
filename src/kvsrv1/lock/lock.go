package lock

import (
	"log"
	"time"

	_ "6.5840/config"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname}
	return lk
}

func (lk *Lock) Acquire() {
	// Give the current client an id
	clientId := kvtest.RandValue(8)
	for {
		// Client try to acquire the lock
		switch cid, version, err := lk.ck.Get(lk.lockname); err {
		case rpc.OK:
			if cid == "" {
				// No one had the lock, try to acquire
				switch err := lk.ck.Put(lk.lockname, clientId, version); err {
				case rpc.ErrVersion:
					// Some client acquire the lock before this, do it later
					time.Sleep(time.Millisecond * 100)
					continue
				case rpc.ErrMaybe:
					// Don't know where the lock is, check it
					continue
				case rpc.OK:
					return
				}
			} else if cid == clientId {
				// The lock owner is this client (when rpc.ErrMaybe may get in the case)
				return
			} else {
				// Some client aready had the lock, do it later
				time.Sleep(time.Millisecond * 100)
				continue
			}
		case rpc.ErrNoKey:
			// Lock state not stored, put it
			lk.ck.Put(lk.lockname, clientId, 0)
		}
	}
}

func (lk *Lock) Release() {
	for {
		// Get the current lock state
		switch cid, version, err := lk.ck.Get(lk.lockname); err {
		case rpc.OK:
			// Aready release the lock
			if cid == "" {
				return
			}
			// Get lock state successfully, update the state
			switch err := lk.ck.Put(lk.lockname, "", version); err {
			case rpc.ErrMaybe:
				continue
			case rpc.OK:
				return
			case rpc.ErrVersion:
				log.Fatalln("Unexpected error")
			}
		case rpc.ErrNoKey:
			log.Fatalln("Should acquire a lock before release")
		}
	}
}
