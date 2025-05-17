package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

func (server *Server) newTxn(ctx *kvrpcpb.Context, version uint64) *mvcc.MvccTxn {
	reader, _ := server.storage.Reader(ctx)
	txn := mvcc.NewMvccTxn(reader, version)
	return txn
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := &kvrpcpb.GetResponse{}
	txn := server.newTxn(req.Context, req.Version)
	key := req.Key
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= txn.StartTS {
		resp.Error = &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				LockTtl:     lock.Ttl,
				Key:         key,
			},
		}
		return resp, nil
	}
	value, err := txn.GetValue(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) writeToStorage(context *kvrpcpb.Context, modifies []storage.Modify) {
	if err := server.storage.Write(context, modifies); err != nil {
		panic(err)
	}
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := &kvrpcpb.PrewriteResponse{}
	resp.Errors = []*kvrpcpb.KeyError{}
	resp.RegionError = nil
	var keys [][]byte

	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	txn := server.newTxn(req.Context, req.StartVersion)

	for _, mutation := range req.Mutations {
		// 1. 检测是否已有锁，若有锁则返回Locked
		lock, err := txn.GetLock(mutation.Key)
		if err != nil {
			return nil, err
		}
		if lock != nil {
			resp.Errors = append(resp.Errors,
				&kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						PrimaryLock: req.PrimaryLock,
						LockVersion: lock.Ts,
						Key:         mutation.Key,
						LockTtl:     req.LockTtl,
					},
				},
			)
		}
		txn.PutLock(mutation.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      txn.StartTS,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		})
	}

	if len(resp.Errors) > 0 || resp.RegionError != nil {
		return resp, nil
	}

	for _, mutation := range req.Mutations {
		// 2. 检测write中是否存在重叠的事务，若有返回WriteConflict
		mostRecentWrite, commitTs, err := txn.MostRecentWrite(mutation.Key)
		if err != nil {
			panic(err)
		}
		if mostRecentWrite != nil && commitTs >= txn.StartTS {
			resp.Errors = append(resp.Errors,
				&kvrpcpb.KeyError{
					Conflict: &kvrpcpb.WriteConflict{
						StartTs:    txn.StartTS,
						ConflictTs: mostRecentWrite.StartTS,
						Key:        mutation.Key,
						Primary:    req.PrimaryLock,
					},
				},
			)
		}
		txn.PutValue(mutation.Key, mutation.Value)
	}

	if len(resp.Errors) > 0 || resp.RegionError != nil {
		return resp, nil
	}

	server.writeToStorage(req.Context, txn.Writes())

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := &kvrpcpb.CommitResponse{}
	resp.Error = nil
	resp.RegionError = nil

	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	txn := server.newTxn(req.Context, req.StartVersion)

	for _, key := range req.Keys {

		write, _, err := txn.MostRecentWrite(key)
		if err != nil {
			panic(err)
		}
		if write != nil && write.StartTS == txn.StartTS {
			if write.Kind == mvcc.WriteKindRollback {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "Transaction has been rollbaked",
				}
				return resp, nil
			}
			continue
		}

		lock, err := txn.GetLock(key)
		if err != nil {
			panic(err)
		}
		if lock == nil {
			// lock不存在且GetValue不存在，说明Prewrite并没有执行
			value, err := txn.GetValue(key)
			if err != nil {
				panic(err)
			}
			if value == nil {
				return resp, nil
			}
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "Lock missed",
			}
			return resp, nil
		}
		if lock.Ts != txn.StartTS {
			resp.Error = &kvrpcpb.KeyError{
				Retryable: "Lock missed",
			}
			return resp, nil
		}

		txn.DeleteLock(key)
		txn.PutWrite(key, req.CommitVersion,
			&mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    lock.Kind,
			},
		)
	}

	server.writeToStorage(req.Context, txn.Writes())

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := &kvrpcpb.ScanResponse{}
	resp.RegionError = nil

	txn := server.newTxn(req.Context, req.Version)

	scanner := mvcc.NewScanner(req.StartKey, txn)

	var result []*kvrpcpb.KvPair

	for len(result) < int(req.Limit) {
		key, val, err := scanner.Next()
		if err != nil {
			panic(err)
		}
		if key == nil {
			break
		}
		if val == nil {
			continue
		}
		result = append(result, &kvrpcpb.KvPair{Key: key, Value: val})
	}
	resp.Pairs = result
	return resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	resp.RegionError = nil

	txn := server.newTxn(req.Context, req.LockTs)

	currentWrite, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		panic(err)
	}
	// 正常commit
	if currentWrite != nil {
		resp.CommitVersion = commitTs
		if currentWrite.Kind == mvcc.WriteKindRollback {
			resp.CommitVersion = 0
		}
		resp.Action = kvrpcpb.Action_NoAction
		return resp, nil
	}

	primaryLock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		panic(err)
	}
	// primaryLock不存在说明已回滚
	// 应标记WriteKindRollBack，本身无需执行任何操作
	if primaryLock == nil {
		txn.PutWrite(req.PrimaryKey, txn.StartTS, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		server.writeToStorage(req.Context, txn.Writes())
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	if primaryLock.Ttl >= mvcc.PhysicalTime(req.CurrentTs)-mvcc.PhysicalTime(req.LockTs) {
		resp.Action = kvrpcpb.Action_NoAction
		resp.LockTtl = primaryLock.Ttl
		return resp, nil
	}

	// 超时，移除prewrite的lock和data
	txn.DeleteLock(req.PrimaryKey)
	txn.DeleteValue(req.PrimaryKey)
	txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
		StartTS: req.LockTs,
		Kind:    mvcc.WriteKindRollback,
	})
	resp.Action = kvrpcpb.Action_TTLExpireRollback

	server.writeToStorage(req.Context, txn.Writes())

	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := &kvrpcpb.BatchRollbackResponse{}
	resp.RegionError = nil

	txn := server.newTxn(req.Context, req.StartVersion)

	for _, key := range req.Keys {
		currentWrite, _, err := txn.CurrentWrite(key)
		if err != nil {
			panic(err)
		}
		if currentWrite != nil {
			if currentWrite.Kind == mvcc.WriteKindRollback {
				continue
			} else {
				resp.Error = &kvrpcpb.KeyError{
					Abort: "The write has been committed",
				}
				return resp, nil
			}
		}
		lock, err := txn.GetLock(key)
		if err != nil {
			panic(err)
		}
		// 锁不存在或被另一个事务占据，标记rollback
		if lock == nil || lock.Ts != txn.StartTS {
			txn.PutWrite(key, txn.StartTS, &mvcc.Write{
				StartTS: txn.StartTS,
				Kind:    mvcc.WriteKindRollback,
			})
			server.writeToStorage(req.Context, txn.Writes())
			return resp, nil
		}

		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, txn.StartTS, &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	server.writeToStorage(req.Context, txn.Writes())

	return resp, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := &kvrpcpb.ResolveLockResponse{}

	txn := server.newTxn(req.Context, req.StartVersion)
	KLs, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		panic(err)
	}

	var keys [][]byte
	for _, KL := range KLs {
		keys = append(keys, KL.Key)
	}

	if req.CommitVersion == 0 {
		server.KvBatchRollback(context.TODO(), &kvrpcpb.BatchRollbackRequest{
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
	} else {
		server.KvCommit(context.TODO(), &kvrpcpb.CommitRequest{
			StartVersion:  req.StartVersion,
			CommitVersion: req.CommitVersion,
			Keys:          keys,
		})
	}
	return resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
