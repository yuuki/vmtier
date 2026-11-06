package config

import "testing"

func TestArchivePackBlockPolicy(t *testing.T) {
	c := good()
	c.MaxPackBlocks = 4
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.Archive(3600000, 7200000).PackLimits.MaxBlocks != 4 {
		t.Fatal("pack block policy not propagated")
	}
	for _, n := range []int{-1, 100001} {
		c.MaxPackBlocks = n
		if err := c.Validate(); err == nil {
			t.Fatalf("invalid block limit %d accepted", n)
		}
	}
}
