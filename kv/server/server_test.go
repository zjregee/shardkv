package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zjregee/shardkv/common/storage"
)

func TestOperationSerialization(t *testing.T) {
	operation := Operation{
		Id:      2,
		Kind:    "TEST_KIND",
		GetKey:  []byte("TEST_KEY"),
		NextKey: []byte("TEST_NEXT_KEY"),
		Writes:  make([]storage.Modify, 0),
	}
	for i := 0; i < 2; i++ {
		data := storage.Put{}
		data.CF = "TEST_CF"
		data.Key = []byte("TEST_KEY")
		data.Value = []byte("TEST_VALUE")
		modify := storage.Modify{}
		modify.Data = data
		operation.Writes = append(operation.Writes, modify)
	}
	for i := 0; i < 2; i++ {
		data := storage.Append{}
		data.CF = "TEST_CF"
		data.Key = []byte("TEST_KEY")
		data.Value = []byte("TEST_VALUE")
		modify := storage.Modify{}
		modify.Data = data
		operation.Writes = append(operation.Writes, modify)
	}
	for i := 0; i < 2; i++ {
		data := storage.Delete{}
		data.CF = "TEST_CF"
		data.Key = []byte("TEST_KEY")
		modify := storage.Modify{}
		modify.Data = data
		operation.Writes = append(operation.Writes, modify)
	}
	serialized := serializeOperation(operation)
	deserialized := deserializeOperation(serialized)
	assert.Equal(t, operation, deserialized)
}
