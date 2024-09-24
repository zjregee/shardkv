package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLockSerialization(t *testing.T) {
	lock := &Lock{}
	lock.Primary = []byte("TEST_KEY")
	lock.StartTs = 2
	lock.Kind = WriteKindPut
	serialized := lock.toBytes()
	deserialized := parseLock(serialized)
	assert.Equal(t, lock, deserialized)
}
