package storage

import (
	"bytes"

	"github.com/petar/GoLLRB/llrb"
)

type MemStorage struct {
	CfDefault *llrb.LLRB
	CfLock    *llrb.LLRB
	CfWrite   *llrb.LLRB
}

func NewMemStorage() *MemStorage {
	s := &MemStorage{}
	s.CfDefault = llrb.New()
	s.CfLock = llrb.New()
	s.CfWrite = llrb.New()
	return s
}

func (s *MemStorage) Write(batch []Modify) {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			item := &memItem{}
			item.key = data.Key
			item.value = data.Value
			switch data.CF {
			case CFDefault:
				s.CfDefault.ReplaceOrInsert(item)
			case CFLock:
				s.CfLock.ReplaceOrInsert(item)
			case CFWrite:
				s.CfWrite.ReplaceOrInsert(item)
			}
		case Append:
			item := &memItem{}
			item.key = data.Key
			exists := s.GetCf(data.CF, data.Key)
			if exists == nil {
				item.value = data.Value
			} else {
				item.value = append(exists, data.Value...)
			}
			switch data.CF {
			case CFDefault:
				s.CfDefault.ReplaceOrInsert(item)
			case CFLock:
				s.CfLock.ReplaceOrInsert(item)
			case CFWrite:
				s.CfWrite.ReplaceOrInsert(item)
			}
		case Delete:
			item := &memItem{}
			item.key = data.Key
			switch data.CF {
			case CFDefault:
				s.CfDefault.Delete(item)
			case CFLock:
				s.CfLock.Delete(item)
			case CFWrite:
				s.CfWrite.Delete(item)
			}
		}
	}
}

func (s *MemStorage) Reader() StorageReader {
	return s
}

func (s *MemStorage) GetCf(cf string, key []byte) []byte {
	item := &memItem{}
	item.key = key
	var result llrb.Item
	switch cf {
	case CFDefault:
		result = s.CfDefault.Get(item)
	case CFLock:
		result = s.CfLock.Get(item)
	case CFWrite:
		result = s.CfWrite.Get(item)
	}
	if result == nil {
		return nil
	}
	return result.(*memItem).value
}

func (s *MemStorage) NextCf(cf string, key []byte) StorageItem {
	var data *llrb.LLRB
	switch cf {
	case CFDefault:
		data = s.CfDefault
	case CFLock:
		data = s.CfLock
	case CFWrite:
		data = s.CfWrite
	}
	var it *memItem
	pivot := &memItem{}
	pivot.key = key
	first := true
	data.AscendGreaterOrEqual(pivot, func(item llrb.Item) bool {
		if first {
			first = false
			return true
		}
		it = item.(*memItem)
		return false
	})
	return it
}

type memItem struct {
	key   []byte
	value []byte
}

func (m *memItem) Key() []byte {
	return m.key
}

func (m *memItem) Value() []byte {
	return m.value
}

func (it *memItem) Less(than llrb.Item) bool {
	other := than.(*memItem)
	return bytes.Compare(it.key, other.key) < 0
}
