package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}

	return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCFFromTxn(sr.txn, cf, key)
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	txn := s.db.NewTransaction(false)

	// 当前无错误可能，这里预留error保证接口通用性
	return &StandAloneStorageReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	/* ===================== （版本1）对batch共同创建一个事务 =====================*/
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch data := m.Data.(type) {
			case storage.Put:
				if err := txn.Set(engine_util.KeyWithCF(data.Cf, data.Key), data.Value); err != nil {
					return err
				}
			case storage.Delete:
				if err := txn.Delete(engine_util.KeyWithCF(data.Cf, data.Key)); err != nil {
					return err
				}
			default:
			}
		}
		return nil
	})
	return err

	/* ===================== （版本2）对每个Modify创建事务 =====================*/
	// for _, m := range batch {
	// 	switch data := m.Data.(type) {
	// 	case storage.Put:
	// 		if err := engine_util.PutCF(s.db, data.Cf, data.Key, data.Value); err != nil {
	// 			return err
	// 		}
	// 	case storage.Delete:
	// 		if err := engine_util.DeleteCF(s.db, data.Cf, data.Key); err != nil {
	// 			return err
	// 		}
	// 	default:
	// 	}
	// }
	// return nil
	/* ===================== （版本2）对每个Modify创建事务 =====================*/
}
