package pack

// EffectiveLimits applies the same defaults and validation used by builders,
// allowing read-only preflight reports to describe the enforced bounds.
func EffectiveLimits(l Limits) (Limits, error) {
	return l.normalized()
}
