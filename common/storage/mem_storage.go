package storage

import (
	"bytes"

	"github.com/petar/GoLLRB/llrb"
	"github.com/zjregee/shardkv/common/utils"
)

type MemStorage struct {
	CfDefault *llrb.LLRB
	CfWrite   *llrb.LLRB
}

func NewMemStorage() *MemStorage {
	s := &MemStorage{}
	s.CfDefault = llrb.New()
	s.CfWrite = llrb.New()
	return s
}

func (s *MemStorage) Write(batch []Modify) error {
	for _, m := range batch {
		switch data := m.Data.(type) {
		case Put:
			item := memItem{}
			item.key = data.Key
			item.value = data.Value
			switch data.CF {
			case CFDefault:
				s.CfDefault.ReplaceOrInsert(item)
			case CFWrite:
				s.CfWrite.ReplaceOrInsert(item)
			}
		case Append:
			item := memItem{}
			item.key = data.Key
			var exists *memItem
			switch data.CF {
			case CFDefault:
				exists = s.CfDefault.Get(item).(*memItem)
			case CFWrite:
				exists = s.CfWrite.Get(item).(*memItem)
			}
			if exists == nil {
				item.value = data.Value
			} else {
				item.value = append(exists.value, data.Value...)
			}
			switch data.CF {
			case CFDefault:
				s.CfDefault.ReplaceOrInsert(item)
			case CFWrite:
				s.CfWrite.ReplaceOrInsert(item)
			}
		case Delete:
			item := memItem{}
			item.key = data.Key
			switch data.CF {
			case CFDefault:
				s.CfDefault.Delete(item)
			case CFWrite:
				s.CfWrite.Delete(item)
			}
		}
	}
	return nil
}

func (s *MemStorage) Reader() (StorageReader, error) {
	return s, nil
}

func (s *MemStorage) GetCF(cf string, key []byte) ([]byte, error) {
	utils.Assert(cf == CFDefault || cf == CFWrite, "cf should be CFDefault or CFWrite")
	item := memItem{}
	item.key = key
	var result llrb.Item
	switch cf {
	case CFDefault:
		result = s.CfDefault.Get(item)
	case CFWrite:
		result = s.CfWrite.Get(item)
	}
	if result == nil {
		return nil, nil
	}
	return result.(memItem).value, nil
}

func (s *MemStorage) IterCF(cf string) StorageIterator {
	utils.Assert(cf == CFDefault || cf == CFWrite, "cf should be CFDefault or CFWrite")
	var data *llrb.LLRB
	switch cf {
	case CFDefault:
		data = s.CfDefault
	case CFWrite:
		data = s.CfWrite
	}
	min := data.Min()
	iter := &memIter{}
	iter.data = data
	if min == nil {
		return iter
	} else {
		iter.item = min.(memItem)
	}
	return iter
}

type memIter struct {
	item memItem
	data *llrb.LLRB
}

func (it *memIter) Valid() bool {
	return it.item.key != nil
}

func (it *memIter) Item() StorageItem {
	return it.item
}

func (it *memIter) Next() {
	pivot := it.item
	it.item = memItem{}
	first := true
	it.data.AscendGreaterOrEqual(pivot, func(item llrb.Item) bool {
		if first {
			first = false
			return true
		}
		it.item = item.(memItem)
		return false
	})
}

func (it *memIter) Seek(key []byte) {
	pivot := memItem{}
	pivot.key = key
	it.item = memItem{}
	it.data.AscendGreaterOrEqual(pivot, func(item llrb.Item) bool {
		it.item = item.(memItem)
		return false
	})
}

type memItem struct {
	key   []byte
	value []byte
}

func (m memItem) Key() []byte {
	return m.key
}

func (m memItem) Value() []byte {
	return m.value
}

func (it memItem) Less(than llrb.Item) bool {
	other := than.(memItem)
	return bytes.Compare(it.key, other.key) < 0
}
