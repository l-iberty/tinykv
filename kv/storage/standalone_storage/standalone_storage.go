package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

type StandAlongStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAlongStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

func (sr *StandAlongStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAlongStorageReader) Close() {
	sr.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	kvPath := filepath.Join(conf.DBPath, "kv")
	raftPath := filepath.Join(conf.DBPath, "raft")

	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)

	return &StandAloneStorage{
		engines: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &StandAlongStorageReader{
		txn: s.engines.Kv.NewTransaction(false),
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			txn := s.engines.Kv.NewTransaction(true)
			cfKey := engine_util.KeyWithCF(m.Cf(), m.Data.(storage.Put).Key)
			value := m.Data.(storage.Put).Value
			if err := txn.Set(cfKey, value); err != nil {
				txn.Discard()
				return err
			}
			if err := txn.Commit(); err != nil {
				txn.Discard()
				return err
			}

		case storage.Delete:
			txn := s.engines.Kv.NewTransaction(true)
			cfKey := engine_util.KeyWithCF(m.Cf(), m.Data.(storage.Delete).Key)
			if err := txn.Delete(cfKey); err != nil {
				txn.Discard()
				return err
			}
			if err := txn.Commit(); err != nil {
				txn.Discard()
				return err
			}
		}
	}
	return nil
}
