package mvcc

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/zjregee/shardkv/common/codec"
	"github.com/zjregee/shardkv/common/storage"
)

type MvccTxn struct {
	Reader  storage.StorageReader
	StartTS uint64
	Writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTS uint64) *MvccTxn {
	txn := &MvccTxn{}
	txn.Reader = reader
	txn.StartTS = startTS
	return txn
}

func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	modify := storage.Modify{
		Data: storage.Put{
			CF:    storage.CFWrite,
			Key:   encodeKey(key, ts),
			Value: write.toBytes(),
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	iter := txn.Reader.IterCF(storage.CFWrite)
	iter.Seek(encodeKey(key, txn.StartTS))
	if !iter.Valid() {
		return nil, nil
	}
	item := iter.Item()
	itemKey := item.Key()
	if !bytes.Equal(decodeKey(itemKey), key) {
		return nil, nil
	}
	itemValue := item.Value()
	write := parseWrite(itemValue)
	if write.Kind == WriteKindDelete {
		return nil, nil
	}
	if write.Kind == WriteKindPut || write.Kind == WriteKindAppend {
		return txn.Reader.GetCF(storage.CFDefault, encodeKey(key, write.StartTS))
	}
	return nil, nil
}

func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	modify := storage.Modify{
		Data: storage.Put{
			CF:    storage.CFDefault,
			Key:   encodeKey(key, txn.StartTS),
			Value: value,
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) DeleteValue(key []byte) {
	modify := storage.Modify{
		Data: storage.Delete{
			CF:  storage.CFDefault,
			Key: encodeKey(key, txn.StartTS),
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(storage.CFWrite)
	iter.Seek(encodeKey(key, math.MaxUint64))
	if !iter.Valid() {
		return nil, 0, nil
	}
	item := iter.Item()
	itemKey := item.Key()
	if !bytes.Equal(decodeKey(itemKey), key) {
		return nil, 0, nil
	}
	itemValue := item.Value()
	return parseWrite(itemValue), decodeTimestamp(itemKey), nil
}

func encodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

func decodeKey(key []byte) []byte {
	_, key, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return key
}

func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}
