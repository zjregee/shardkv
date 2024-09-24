package server

import (
	"bytes"
	"encoding/gob"
	"fmt"

	l "github.com/zjregee/shardkv/common/logger"
	"github.com/zjregee/shardkv/common/storage"
	pb "github.com/zjregee/shardkv/proto"
)

const (
	OperationKindGet   = "GET"
	OperationKindNext  = "NEXT"
	OperationKindWrite = "WRITE"
)

type Operation struct {
	Id      int64
	Kind    string
	GetCf   string
	NextCf  string
	GetKey  []byte
	NextKey []byte
	Writes  []storage.Modify
}

type OperationResult struct {
	err   pb.Err
	key   []byte
	value []byte
}

var _ storage.Storage = (*Server)(nil)
var _ storage.StorageReader = (*Server)(nil)
var _ storage.StorageItem = (*OperationResult)(nil)

func (or OperationResult) Key() []byte {
	return or.key
}

func (or OperationResult) Value() []byte {
	return or.value
}

func serializeOperation(operation Operation) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(operation)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func deserializeOperation(data []byte) Operation {
	var op Operation
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&op)
	if err != nil {
		panic(err)
	}
	return op
}

func (kv *Server) Write(batch []storage.Modify) {
	op := Operation{}
	op.Kind = OperationKindWrite
	op.Writes = batch
	_ = kv.submitOperation(op)
}

func (kv *Server) Reader() storage.StorageReader {
	return kv
}

func (kv *Server) GetCf(cf string, key []byte) []byte {
	op := Operation{}
	op.Kind = OperationKindGet
	op.GetKey = key
	result := kv.submitOperation(op)
	if result.err != pb.Err_OK {
		return nil
	}
	return result.value
}

func (kv *Server) NextCf(cf string, key []byte) storage.StorageItem {
	op := Operation{}
	op.Kind = OperationKindNext
	op.NextKey = key
	result := kv.submitOperation(op)
	if result.err != pb.Err_OK {
		return nil
	}
	return result
}

func (kv *Server) submitOperation(op Operation) OperationResult {
	entry := serializeOperation(op)
	kv.rf.Start(entry)
	ch := make(chan OperationResult)
	kv.mu.Lock()
	kv.notifyMap[op.Id] = ch
	kv.mu.Unlock()
	var result OperationResult
	dead := false
	select {
	case result = <-ch:
	case <-kv.deadCh:
		dead = true
	}
	close(ch)
	kv.mu.Lock()
	delete(kv.notifyMap, op.Id)
	kv.mu.Unlock()
	if dead {
		result.err = pb.Err_ErrClosed
	}
	return result
}

func (kv *Server) recordWrites(op Operation) {
	var record string
	for _, write := range op.Writes {
		switch write := write.Data.(type) {
		case storage.Put:
			record = fmt.Sprintf("Put(%s, %s, %s)", write.CF, write.Key, write.Value)
		case storage.Append:
			record = fmt.Sprintf("Append(%s, %s, %s)", write.CF, write.Key, write.Value)
		case storage.Delete:
			record = fmt.Sprintf("Delete(%s, %s)", write.CF, write.Key)
		}
		l.Log.Infof("[node %d] apply operation: %s", kv.me, record)
	}
}

func (kv *Server) recordOperation(op Operation) {
	if op.Kind == OperationKindWrite {
		kv.recordWrites(op)
		return
	}
	var record string
	switch op.Kind {
	case OperationKindGet:
		record = fmt.Sprintf("%s(id: %d, key: %s)", op.Kind, op.Id, op.GetKey)
	case OperationKindNext:
		record = fmt.Sprintf("%s(id: %d, key: %s)", op.Kind, op.Id, op.NextKey)
	}
	l.Log.Infof("[node %d] apply operation: %s", kv.me, record)
}

func (kv *Server) applyOperation(op Operation) {
	_, needNotify := kv.notifyMap[op.Id]
	if needNotify {
		result := OperationResult{}
		switch op.Kind {
		case OperationKindGet:
			value := kv.storage.Reader().GetCf(op.GetCf, op.GetKey)
			if value == nil {
				result.err = pb.Err_ErrNoKey
			} else {
				result.err = pb.Err_OK
				result.value = value
			}
		case OperationKindNext:
			item := kv.storage.Reader().NextCf(op.NextCf, op.NextKey)
			if item == nil {
				result.err = pb.Err_ErrNoKey
			} else {
				result.err = pb.Err_OK
				result.key = item.Key()
				result.value = item.Value()
			}
		case OperationKindWrite:
			_, isInExecute := kv.executeMap[op.Id]
			if !isInExecute {
				kv.storage.Write(op.Writes)
				kv.executeMap[op.Id] = struct{}{}
				result.err = pb.Err_OK
			} else {
				result.err = pb.Err_Duplicate
			}
		}
		ch := kv.notifyMap[op.Id]
		select {
		case ch <- result:
		default:
		}
	} else if op.Kind == OperationKindWrite {
		kv.storage.Write(op.Writes)
	}
}
