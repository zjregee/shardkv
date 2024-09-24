package transaction

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/zjregee/shardkv/common/codec"
	"github.com/zjregee/shardkv/common/storage"
)

type WriteKind int

const (
	WriteKindPut      WriteKind = 1
	WriteKindAppend   WriteKind = 2
	WriteKindDelete   WriteKind = 3
	WriteKindRollback WriteKind = 4
)

type MvccTxn struct {
	Reader  storage.StorageReader
	StartTs uint64
	Writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	txn := &MvccTxn{}
	txn.Reader = reader
	txn.StartTs = startTs
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

func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	value := txn.Reader.GetCf(storage.CFLock, key)
	if value == nil {
		return nil, nil
	}
	return parseLock(value), nil
}

func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	modify := storage.Modify{
		Data: storage.Put{
			CF:    storage.CFLock,
			Key:   key,
			Value: lock.toBytes(),
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) DeleteLock(key []byte) {
	modify := storage.Modify{
		Data: storage.Delete{
			CF:  storage.CFLock,
			Key: key,
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) GetValue(key []byte) []byte {
	item := txn.Reader.NextCf(storage.CFWrite, encodeKey(key, txn.StartTs))
	itemKey := item.Key()
	if !bytes.Equal(decodeKey(itemKey), key) {
		return nil
	}
	itemValue := item.Value()
	write := parseWrite(itemValue)
	if write.Kind == WriteKindDelete {
		return nil
	}
	if write.Kind == WriteKindPut || write.Kind == WriteKindAppend {
		return txn.Reader.GetCf(storage.CFDefault, encodeKey(key, write.StartTs))
	}
	return nil
}

func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	modify := storage.Modify{
		Data: storage.Put{
			CF:    storage.CFDefault,
			Key:   encodeKey(key, txn.StartTs),
			Value: value,
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) AppendValue(key []byte, value []byte) {
	modify := storage.Modify{
		Data: storage.Append{
			CF:    storage.CFDefault,
			Key:   encodeKey(key, txn.StartTs),
			Value: value,
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) DeleteValue(key []byte) {
	modify := storage.Modify{
		Data: storage.Delete{
			CF:  storage.CFDefault,
			Key: encodeKey(key, txn.StartTs),
		},
	}
	txn.Writes = append(txn.Writes, modify)
}

func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64) {
	value := txn.Reader.GetCf(storage.CFDefault, encodeKey(key, txn.StartTs))
	if value == nil {
		return nil, 0
	}
	item := txn.Reader.NextCf(storage.CFWrite, encodeKey(key, math.MaxUint64))
	for item != nil {
		itemKey := item.Key()
		if !bytes.Equal(decodeKey(itemKey), key) {
			return nil, 0
		}
		itemValue := item.Value()
		write := parseWrite(itemValue)
		if write.StartTs == txn.StartTs {
			return write, decodeTimestamp(itemKey)
		}
		item = txn.Reader.NextCf(storage.CFWrite, itemKey)
	}
	return nil, 0
}

func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64) {
	item := txn.Reader.NextCf(storage.CFWrite, encodeKey(key, math.MaxUint64))
	itemKey := item.Key()
	if !bytes.Equal(decodeKey(itemKey), key) {
		return nil, 0
	}
	itemValue := item.Value()
	return parseWrite(itemValue), decodeTimestamp(itemKey)
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
