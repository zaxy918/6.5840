package kvraft

import (
	"log/slog"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

const RETRY_INTERVAL = time.Millisecond * 100

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
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
	for {
		// Construct args and reply
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		// Do rpc
		if ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply); ok {
			switch reply.Err {
			case rpc.OK:
				slog.Debug(
					"CLIENT_GET",
					"PEER", leader,
					"EVENT", "GET_SUCCESS",
				)
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				slog.Debug(
					"CLIENT_GET",
					"PEER", leader,
					"EVENT", "GET_NO_KEY",
				)
				return "", 0, reply.Err
			case rpc.ErrWrongLeader:
				// Try next server
				slog.Debug(
					"CLIENT_GET",
					"PEER", leader,
					"EVENT", "GET_WRONG_LEADER",
				)
				ck.mu.Lock()
				if ck.leader == leader {
					ck.leader = (ck.leader + 1) % len(ck.servers)
				}
				ck.mu.Unlock()
				time.Sleep(RETRY_INTERVAL)
				continue
			default:
				slog.Debug(
					"CLIENT_GET",
					"PEER", leader,
					"EVENT", "GET_ERROR",
				)
				time.Sleep(RETRY_INTERVAL)
				continue
			}
		} else {
			// Rpc not succeed
			time.Sleep(RETRY_INTERVAL)
			slog.Debug(
				"CLIENT_GET",
				"PEER", leader,
				"EVENT", "GET_RPC_FAIL",
			)
			continue
		}
	}
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
	// If the client is first do the rpc
	firstCall := true
	for {
		// Construct args and reply
		args := rpc.PutArgs{Key: key, Value: value, Version: version}
		reply := rpc.PutReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		// Do rpc
		if ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply); ok {
			if reply.Err == rpc.ErrWrongLeader {
				slog.Debug(
					"CLIENT_PUT",
					"PEER", leader,
					"EVENT", "PUT_WRONG_LEADER",
				)
				ck.mu.Lock()
				if ck.leader == leader {
					ck.leader = (ck.leader + 1) % len(ck.servers)
				}
				ck.mu.Unlock()
				time.Sleep(RETRY_INTERVAL)
				continue
			}
			if !firstCall && reply.Err == rpc.ErrVersion {
				slog.Debug(
					"CLIENT_PUT",
					"PEER", leader,
					"EVENT", "PUT_RESEND_ERR_VERSION",
				)
				// A resend call with ErrVersion
				return rpc.ErrMaybe
			} else {
				slog.Debug(
					"CLIENT_PUT",
					"PEER", leader,
					"EVENT", "PUT_SUCCESS",
				)
				return reply.Err
			}
		} else if firstCall {
			// Do call later
			slog.Debug(
				"CLIENT_PUT",
				"PEER", leader,
				"EVENT", "PUT_FIRST_CALL_FAIL",
			)
			time.Sleep(RETRY_INTERVAL)
			firstCall = false
			continue
		} else {
			// Two call fail
			slog.Debug(
				"CLIENT_PUT",
				"PEER", leader,
				"EVENT", "PUT_RECALL_FAIL",
			)
			return rpc.ErrMaybe
		}
	}
}
