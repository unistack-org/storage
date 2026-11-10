package crushmap

import (
	"io/ioutil"
	"testing"
)

func TestText(t *testing.T) {
	buf, err := ioutil.ReadFile("testdata/map.txt")
	if err != nil {
		t.Fatal(err)
	}

	m := NewMap()
	err = m.DecodeText(buf)
	if err != nil {
		t.Fatal(err)
	}
	_ = m
}
