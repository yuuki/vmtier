package config

import "errors"

// perReplicaStore allocates a fixed share of the configured aggregate budget.
// The operator must bound simultaneous clients (including draining replacements
// and maintenance). No distributed admission or borrowing occurs here. Return a
// copy so repeated validation/open never divides the configured totals twice.
func (c *Config) perReplicaStore() (Store, error) {
	store := c.Store
	replicas := c.Service.MaxReplicas
	if c.Catalog.Type != "postgres" {
		if replicas != 0 {
			return store, errors.New("service.max_replicas requires a PostgreSQL catalog")
		}
		return store, nil
	}
	if replicas < 1 || replicas > 100 {
		return store, errors.New("PostgreSQL service requires explicit service.max_replicas between 1 and 100")
	}
	if store.MaxConcurrentRequests < replicas || store.MaxQueuedRequests < replicas {
		return store, errors.New("aggregate S3 concurrency and queue limits must each provide at least one slot per replica")
	}
	store.RequestsPerSecond /= float64(replicas)
	store.MaxConcurrentRequests /= replicas
	store.MaxQueuedRequests /= replicas
	return store, nil
}
