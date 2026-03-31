package ago_test

import (
	"testing"

	"github.com/zoobz-io/ago"
)

func TestNoInput(_ *testing.T) {
	// NoInput should be usable as a zero-value struct.
	var ni ago.NoInput
	_ = ni
}

func TestNoOutput(_ *testing.T) {
	// NoOutput should be usable as a zero-value struct.
	var no ago.NoOutput
	_ = no
}
