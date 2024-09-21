package server

import (
	"github.com/zjregee/shardkv/common/storage"
)

func (kv *Server) Write(batch []storage.Modify) error {
	return nil
}

func (kv *Server) Reader() (storage.StorageReader, error) {
	return nil, nil
}

func (kv *Server) GetCF(cf string, key []byte) ([]byte, error) {
	return nil, nil
}

func (kv *Server) IterCF(cf string) storage.StorageIterator {
	return nil
}

func (kv *Server) Valid() bool {
	return false
}

func (kv *Server) Item() storage.StorageItem {
	return nil
}

func (kv *Server) Next() {}

func (kv *Server) Seek(key []byte) {}
