package common

import (
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

const (
	CFDefault  string = "default"
	CFWrite    string = "write"
	CFLock     string = "lock"
	CFMetadata string = "metadata"
)

var CFs [3]string = [3]string{CFDefault, CFWrite, CFLock}

type WriteEntry struct {
	CF    string
	Key   []byte
	Value []byte
}

type WriteBatch struct {
	entries []*WriteEntry
}

func (wb *WriteBatch) SetCF(cf string, key []byte, value []byte) {
	entry := &WriteEntry{CF: cf, Key: key, Value: value}
	wb.entries = append(wb.entries, entry)
}

func (wb *WriteBatch) DeleteCF(cf string, key []byte) {
	entry := &WriteEntry{CF: cf, Key: key, Value: nil}
	wb.entries = append(wb.entries, entry)
}

func (wb *WriteBatch) WriteToDB(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		for _, entry := range wb.entries {
			bucket, err := tx.CreateBucketIfNotExists([]byte(entry.CF))
			if err != nil {
				return err
			}
			if entry.Value == nil {
				err = bucket.Delete(entry.Key)
				if err != nil {
					return err
				}
			} else {
				err = bucket.Put(entry.Key, entry.Value)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

type Engines struct {
	Kv   *bbolt.DB
	Raft *bbolt.DB
}

func NewEngines(kvDB *bbolt.DB, raftDB *bbolt.DB) *Engines {
	engines := &Engines{kvDB, raftDB}
	return engines
}

func (en *Engines) GetMetadata(key []byte) ([]byte, error) {
	return en.GetFromKv(CFMetadata, key)
}

func (en *Engines) SetMetadata(key []byte, value []byte) error {
	wb := &WriteBatch{}
	wb.SetCF(CFMetadata, key, value)
	return en.WriteToKv(wb)
}

func (en *Engines) GetFromKv(cf string, key []byte) ([]byte, error) {
	var value []byte
	err := en.Kv.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(cf))
		if bucket == nil {
			return nil
		}
		value = bucket.Get(key)
		return nil
	})
	return value, err
}

func (en *Engines) GetFromRaft(cf string, key []byte) ([]byte, error) {
	var value []byte
	err := en.Raft.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(cf))
		if bucket == nil {
			return nil
		}
		value = bucket.Get(key)
		return nil
	})
	return value, err
}

func (en *Engines) WriteToKv(wb *WriteBatch) error {
	return wb.WriteToDB(en.Kv)
}

func (en *Engines) WriteToRaft(wb *WriteBatch) error {
	return wb.WriteToDB(en.Raft)
}

func (en *Engines) Close() error {
	if err := en.Kv.Close(); err != nil {
		panic(err)
	}
	if err := en.Raft.Close(); err != nil {
		panic(err)
	}
	return nil
}

func (en *Engines) Destroy() error {
	if err := en.Kv.Close(); err != nil {
		panic(err)
	}
	if err := en.Raft.Close(); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(en.Kv.Path()); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(en.Raft.Path()); err != nil {
		panic(err)
	}
	return nil
}

func CreateEngine(dir, name string) *bbolt.DB {
	if err := createDirIfNotExists(dir); err != nil {
		panic(err)
	}
	path := filepath.Join(dir, name)
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		panic(err)
	}
	return db
}

func createDirIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}
