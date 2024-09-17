package mvcc

import (
	"encoding/binary"

	"github.com/zjregee/shardkv/common"
)

type Write struct {
	StartTS uint64
	Kind    WriteKind
}

func (wr *Write) ToBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	common.Assert(len(buf) == 9, "len(buf) == 9")
	binary.BigEndian.PutUint64(buf[1:], wr.StartTS)
	return buf
}

func ParseWrite(value []byte) *Write {
	common.Assert(len(value) == 9, "len(value) == 9")
	kind := WriteKind(value[0])
	startTs := binary.BigEndian.Uint64(value[1:])
	write := &Write{startTs, kind}
	return write
}

type WriteKind int

const (
	WriteKindPut      WriteKind = 1
	WriteKindDelete   WriteKind = 2
	WriteKindRollback WriteKind = 3
)
