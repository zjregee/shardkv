package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTestMemStorage() *MemStorage {
	s := NewMemStorage()
	writes := make([]Modify, 0)
	for i := 0; i < 2; i++ {
		data := Put{}
		data.CF = CFDefault
		data.Key = append([]byte("TEST_KEY_"), byte(i))
		data.Value = append([]byte("TEST_VALUE_"), byte(i))
		modify := Modify{}
		modify.Data = data
		writes = append(writes, modify)
	}
	s.Write(writes)
	return s
}

func TestMemStorageGet(t *testing.T) {
	s := getTestMemStorage()
	for i := 0; i < 2; i++ {
		key := append([]byte("TEST_KEY_"), byte(i))
		value := s.GetCf(CFDefault, key)
		assert.Equal(t, append([]byte("TEST_VALUE_"), byte(i)), value)
	}
}

func TestMemStorageAppend(t *testing.T) {
	s := getTestMemStorage()
	writes := make([]Modify, 0)
	for i := 0; i < 2; i++ {
		data := Append{}
		data.CF = CFDefault
		data.Key = append([]byte("TEST_KEY_"), byte(i))
		data.Value = []byte("TEST_APPEND_CONTENT")
		modify := Modify{}
		modify.Data = data
		writes = append(writes, modify)
	}
	s.Write(writes)
	for i := 0; i < 2; i++ {
		key := append([]byte("TEST_KEY_"), byte(i))
		value := s.GetCf(CFDefault, key)
		assert.Equal(t, append(append([]byte("TEST_VALUE_"), byte(i)), []byte("TEST_APPEND_CONTENT")...), value)
	}
}

func TestMemStorageDelete(t *testing.T) {
	s := getTestMemStorage()
	writes := make([]Modify, 0)
	for i := 0; i < 2; i++ {
		data := Delete{}
		data.CF = CFDefault
		data.Key = append([]byte("TEST_KEY_"), byte(i))
		modify := Modify{}
		modify.Data = data
		writes = append(writes, modify)
	}
	s.Write(writes)
	for i := 0; i < 2; i++ {
		key := append([]byte("TEST_KEY_"), byte(i))
		value := s.GetCf(CFDefault, key)
		assert.Nil(t, value)
	}
}

func TestMemStorageNext(t *testing.T) {
	s := getTestMemStorage()
	key := append([]byte("TEST_KEY_"), byte(0))
	item := s.NextCf(CFDefault, key)
	assert.Equal(t, append([]byte("TEST_KEY_"), byte(1)), item.Key())
	assert.Equal(t, append([]byte("TEST_VALUE_"), byte(1)), item.Value())
	key = append([]byte("TEST_KEY_"), byte(1))
	item = s.NextCf(CFDefault, key)
	assert.Nil(t, item)
}
