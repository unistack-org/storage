package cpu

import (
	"reflect"
	"testing"
)

func TestParseCPUs(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []int
	}{
		{"0", []int{0}},
		{"0,2", []int{0, 2}},
		{"2,0", []int{0, 2}},
		{"0-3", []int{0, 1, 2, 3}},
		{"3-0", []int{0, 1, 2, 3}},
		{"0-3,8,10-11", []int{0, 1, 2, 3, 8, 10, 11}},
		{"1,1,2", []int{1, 2}},
		{" 0-1 , 3 ", []int{0, 1, 3}},
	} {
		got, err := ParseCPUs(tc.in)
		if err != nil {
			t.Errorf("ParseCPUs(%q): %v", tc.in, err)
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("ParseCPUs(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseCPUsErrors(t *testing.T) {
	for _, in := range []string{"", "  ", "a", "1,", "-1", "1-a", "0-100000"} {
		if _, err := ParseCPUs(in); err == nil {
			t.Errorf("ParseCPUs(%q): expected error", in)
		}
	}
}

func TestAvailable(t *testing.T) {
	if Available() < 1 {
		t.Fatalf("Available() = %d", Available())
	}
}
