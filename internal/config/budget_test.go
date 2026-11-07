package config

import "testing"

func TestPostgresStaticBudgetAllocation(t *testing.T) {
	c := postgresConfig()
	c.Service.MaxReplicas = 3
	c.Store.RequestsPerSecond = 80
	c.Store.MaxConcurrentRequests = 8
	c.Store.MaxQueuedRequests = 64
	want := c.Store
	for i := 0; i < 2; i++ {
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		store, err := c.perReplicaStore()
		if err != nil {
			t.Fatal(err)
		}
		if store.RequestsPerSecond != 80.0/3 || store.MaxConcurrentRequests != 2 || store.MaxQueuedRequests != 21 {
			t.Fatalf("unexpected share: %+v", store)
		}
		if c.Store != want {
			t.Fatal("validation/allocation changed configured total")
		}
	}
}

func TestPostgresStaticBudgetRejectsOversubscription(t *testing.T) {
	for _, replicas := range []int{-1, 0, 101, 3} {
		c := postgresConfig()
		c.Service.MaxReplicas = replicas
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted invalid allocation for %d replicas", replicas)
		}
	}
	c := postgresConfig()
	c.Service.MaxReplicas = 2
	c.Store.MaxQueuedRequests = 1
	if err := c.Validate(); err == nil {
		t.Fatal("accepted fewer queued slots than replicas")
	}
	c = good()
	c.Service.MaxReplicas = 2
	if err := c.Validate(); err == nil {
		t.Fatal("silently applied aggregate allocation to legacy backend")
	}
}
