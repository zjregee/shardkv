package mvcc

import (
	"encoding/binary"

	"github.com/zjregee/shardkv/common/utils"
)

type Write struct {
	StartTS uint64
	Kind    WriteKind
}

func (wr *Write) toBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	utils.Assert(len(buf) == 9, "len(buf) should be 9")
	binary.BigEndian.PutUint64(buf[1:], wr.StartTS)
	return buf
}

func parseWrite(value []byte) *Write {
	utils.Assert(len(value) == 9, "len(value) should be 9")
	kind := WriteKind(value[0])
	startTs := binary.BigEndian.Uint64(value[1:])
	write := &Write{}
	write.StartTS = startTs
	write.Kind = kind
	return write
}

type WriteKind int

const (
	WriteKindPut    WriteKind = 1
	WriteKindAppend WriteKind = 2
	WriteKindDelete WriteKind = 3
)
