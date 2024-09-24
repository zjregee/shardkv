package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteSerialization(t *testing.T) {
	write := &Write{}
	write.StartTs = 2
	write.Kind = WriteKindPut
	serialized := write.toBytes()
	deserialized := parseWrite(serialized)
	assert.Equal(t, write, deserialized)
}
