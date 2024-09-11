package common

import (
	"crypto/rand"
	"math/big"
)

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func Assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
