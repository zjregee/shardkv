package storage

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

type StorageIterator interface {
	Valid() bool
	Item() StorageItem
	Next()
	Seek(key []byte)
}

type StorageReader interface {
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) StorageIterator
}

type Storage interface {
	Write(batch []Modify) error
	Reader() (StorageReader, error)
}

const (
	CFDefault = "DEFAULT"
	CFLock    = "LOCK"
	CFWrite   = "WRITE"
)
