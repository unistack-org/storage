package crush

import (
	"io/ioutil"
	"testing"
)

func TestJson(t *testing.T) {
	buf, err := ioutil.ReadFile("testdata/map.json")
	if err != nil {
		t.Fatal(err)
	}

	m := NewWrapper()
	err = m.DecodeJson(buf)
	if err != nil {
		t.Fatal(err)
	}
	_ = m
}
