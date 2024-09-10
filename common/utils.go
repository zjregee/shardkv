package common

import (
	"os"
)

func Assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}

func createDirIfNotExists(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return nil
}
