package server

import (
	"context"
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, _ := server.storage.Reader(nil)
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, fmt.Errorf("RawGet failed: %v", err)
	}
	if value == nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		Value: value,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	}
	err := server.storage.Write(nil, batch)
	// 设计思考：错误仅通过error返回是否足够？
	if err != nil {
		return nil, fmt.Errorf("RawPut failed: %v", err)
	}
	return nil, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, fmt.Errorf("RawDelete failed: %v", err)
	}
	return nil, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, _ := server.storage.Reader(nil)
	iter := reader.IterCF(req.Cf)
	if len(req.StartKey) > 0 {
		iter.Seek(req.StartKey)
	}
	var kvs []*kvrpcpb.KvPair
	// 这里应该使用Copy语义避免切片带来的不一致性问题
	for iter.Valid() {
		key := iter.Item().KeyCopy(nil)
		value, err := iter.Item().ValueCopy(nil)
		if err != nil {
			return nil, fmt.Errorf("RawScan value copy filed: %v", err)
		}
		kvs = append(kvs,
			&kvrpcpb.KvPair{
				Key:   key,
				Value: value,
			},
		)
		if len(kvs) >= int(req.Limit) {
			break
		}
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
