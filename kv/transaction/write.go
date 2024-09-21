package transaction

import (
	"encoding/binary"

	"github.com/zjregee/shardkv/common/utils"
)

type Write struct {
	StartTs uint64
	Kind    WriteKind
}

func (wr *Write) toBytes() []byte {
	buf := append([]byte{byte(wr.Kind)}, 0, 0, 0, 0, 0, 0, 0, 0)
	utils.Assert(len(buf) == 9, "len(buf) should be 9")
	binary.BigEndian.PutUint64(buf[1:], wr.StartTs)
	return buf
}

func parseWrite(value []byte) *Write {
	utils.Assert(len(value) == 9, "len(value) should be 9")
	kind := WriteKind(value[0])
	startTs := binary.BigEndian.Uint64(value[1:])
	write := &Write{}
	write.StartTs = startTs
	write.Kind = kind
	return write
}
