package mvcc

import (
	"encoding/binary"
)

type Lock struct {
	Primary []byte
	StartTs uint64
	Kind    WriteKind
}

func (lock *Lock) toBytes() []byte {
	buf := append(lock.Primary, byte(lock.Kind), 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[len(lock.Primary)+1:], lock.StartTs)
	return buf
}

func parseLock(input []byte) *Lock {
	primaryLen := len(input) - 9
	primary := input[:primaryLen]
	kind := WriteKind(input[primaryLen])
	startTs := binary.BigEndian.Uint64(input[primaryLen+1:])
	lock := &Lock{}
	lock.Primary = primary
	lock.StartTs = startTs
	lock.Kind = kind
	return lock
}
