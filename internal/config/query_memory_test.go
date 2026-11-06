package config

import "testing"

func TestQueryMemoryAdmissionConfiguration(t *testing.T) {
	c := good()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	l := c.GatewayLimits()
	if l.QueryWorkBytes != 128<<20 || l.QueryMemoryBytes != 256<<20 {
		t.Fatal("unexpected query admission defaults")
	}
	for _, invalid := range []int64{-1, 1, (8 << 30) + 1} {
		c.QueryMemoryBytes = invalid
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted invalid shared budget %d", invalid)
		}
	}
	c.MaxQueryBytes, c.QueryMemoryBytes = 64<<20, 64<<20
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	l = c.GatewayLimits()
	if l.QueryWorkBytes != 64<<20 || l.QueryMemoryBytes != 64<<20 {
		t.Fatal("custom query reservation ignored")
	}
}
