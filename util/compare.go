package util

import (
	"os"
)

func CheckResult(fileBuf []byte, path string) bool {
	f, err := os.ReadFile(path)

	if err != nil {
		return false
	}

	if len(f) != len(fileBuf) {
		return false
	}

	for i := 0; i < len(f); i++ {
		if f[i] != fileBuf[i] {
			return false
		}
	}

	return true
}
