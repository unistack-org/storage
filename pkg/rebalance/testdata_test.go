package rebalance

import (
	"os"
	"path/filepath"
	"testing"
)

func readTestdata(t *testing.T) ([]byte, error) {
	t.Helper()
	return os.ReadFile(filepath.Join("..", "..", "crush", "testdata", "map.txt"))
}
