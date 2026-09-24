package kvsrv

import (
	"log/slog"
	"sync"

	_ "6.5840/config"
	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// const Debug = false

//	func DPrintf(format string, a ...any) (n int, err error) {
//		if Debug {
//			log.Printf(format, a...)
//		}
//		return
//	}
type VV struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu  sync.Mutex
	kvv map[string]VV
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvv = make(map[string]VV)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	slog.Debug("Server do Get", "args", args)
	// Parse args
	key := args.Key
	if vv, ok := kv.kvv[key]; ok {
		// Key exist
		*reply = rpc.GetReply{Value: vv.value, Version: vv.version, Err: rpc.OK}
	} else {
		// Key not exist
		*reply = rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
	}
	slog.Debug("Server Put reply with", "reply", reply)
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	slog.Debug("Server do Put", "args", args)
	// Parse args
	key := args.Key
	avv := VV{args.Value, args.Version}
	if vv, ok := kv.kvv[key]; ok {
		if vv.version == avv.version {
			// Find the key with right version, put value with version + 1
			kv.kvv[key] = VV{avv.value, avv.version + 1}
			*reply = rpc.PutReply{Err: rpc.OK}
		} else {
			// Version wrong
			*reply = rpc.PutReply{Err: rpc.ErrVersion}
		}
	} else if avv.version == 0 {
		kv.kvv[key] = VV{avv.value, avv.version + 1}
		*reply = rpc.PutReply{Err: rpc.OK}
	} else {
		*reply = rpc.PutReply{Err: rpc.ErrNoKey}
	}
	slog.Debug("Server Put reply with", "reply", reply)
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
