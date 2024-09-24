package storage

import "encoding/gob"

type Modify struct {
	Data interface{}
}

type Put struct {
	CF    string
	Key   []byte
	Value []byte
}

type Append struct {
	CF    string
	Key   []byte
	Value []byte
}

type Delete struct {
	CF  string
	Key []byte
}

type StorageItem interface {
	Key() []byte
	Value() []byte
}

type StorageReader interface {
	GetCf(cf string, key []byte) []byte
	NextCf(cf string, key []byte) StorageItem
}

type Storage interface {
	Write(batch []Modify)
	Reader() StorageReader
}

const (
	CFDefault = "DEFAULT"
	CFLock    = "LOCK"
	CFWrite   = "WRITE"
)

func init() {
	gob.Register(Put{})
	gob.Register(Append{})
	gob.Register(Delete{})
}
