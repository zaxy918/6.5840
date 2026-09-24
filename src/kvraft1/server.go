package kvraft

import (
	"log/slog"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type VV struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	kvv map[string]VV
	mu  sync.Mutex
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch req := req.(type) {
	case rpc.GetArgs:
		slog.Debug(
			"SERVER_DO_OP",
			"PEER", kv.me,
			"EVENT", "GET",
		)
		// Key exists, return value
		if vv, ok := kv.kvv[req.Key]; ok {
			return rpc.GetReply{Value: vv.value, Version: vv.version, Err: rpc.OK}
		}
		// Key doesn't exist, return ErrNoKey
		return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
	case rpc.PutArgs:
		slog.Debug(
			"SERVER_DO_OP",
			"PEER", kv.me,
			"EVENT", "PUT",
		)
		// Key exists, check version
		if vv, ok := kv.kvv[req.Key]; ok {
			if vv.version == req.Version {
				kv.kvv[req.Key] = VV{value: req.Value, version: vv.version + 1}
				return rpc.PutReply{Err: rpc.OK}
			} else {
				return rpc.PutReply{Err: rpc.ErrVersion}
			}
		}
		// Key doesn't exist, check version
		if req.Version == 0 {
			kv.kvv[req.Key] = VV{value: req.Value, version: 1}
			return rpc.PutReply{Err: rpc.OK}
		} else {
			return rpc.PutReply{Err: rpc.ErrNoKey}
		}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	slog.Debug(
		"SERVER_GET",
		"PEER", kv.me,
		"EVENT", "SUBMIT",
	)
	if err, res := kv.rsm.Submit(*args); err != rpc.OK {
		reply.Err = err
	} else {
		*reply = res.(rpc.GetReply)
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	slog.Debug(
		"SERVER_PUT",
		"PEER", kv.me,
		"EVENT", "SUBMIT",
	)
	if err, res := kv.rsm.Submit(*args); err != rpc.OK {
		reply.Err = err
	} else {
		*reply = res.(rpc.PutReply)
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, kvv: make(map[string]VV)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
