/-!
# The timing contract of vmtier

`docs/design.md` ("Data and time contract") fixes archive age `H`, enforced
lateness `L`, Hot retention `R`, safety margin `s` and window width `w` with

    H > L ≥ 0,  s > 0,  R > H + s + w,
    eligible_end(now) = floor((now - H) / w) * w

and claims that a query vmtier answers never misses an accepted sample:
Cold serves the windows below its coverage end, Hot what it still retains,
and a gap between them is an error. This file proves that claim for the
arithmetic the code performs, over all integers:

* `requiredEnd` is `cold.Reader.RequiredEnd` (and the archiver's
  `eligibleEnd`). Go's `/` truncates toward zero, so it is `Int.tdiv`, not
  Lean's `/`.
* `timeValid` is `archive.Config.validateTime`, which the archiver checks
  before export and again before publication.
* `gatewayAccepts` is the coverage check of `cold.Reader.Search`.

What is assumed, not proved: Hot retains every sample younger than `R`;
ingress enforces `L`; export plus oracle verification return exactly the
samples Hot holds; and the catalog's coverage is contiguous, with one
complete window per index (checked by `spec/VmtierPublication.tla`).
All quantities are UTC milliseconds.
-/

namespace VmtierTiming

/-- The configuration `archive.Config.validateConfig` and `passConfig`
accept. -/
structure Contract where
  H : Int
  L : Int
  R : Int
  s : Int
  w : Int
  lateness_nonneg : 0 ≤ L
  lateness_lt_age : L < H
  safety_pos : 0 < s
  window_pos : 0 < w
  retention_headroom : H + s + w < R

/-- `cold.Reader.RequiredEnd(now)`: `(now - H) / w * w` with Go's
truncating division. The archiver's `eligibleEnd` is the same expression. -/
def requiredEnd (c : Contract) (now : Int) : Int :=
  Int.tdiv (now - c.H) c.w * c.w

/-- `archive.Config.validateTime(start, end, now)`: the window holds no
sample younger than `H`, and its start is not expired or inside the safety
margin of Hot retention. -/
def timeValid (c : Contract) (start stop now : Int) : Prop :=
  stop ≤ now - c.H ∧ ¬ start ≤ now - c.R + c.s

/-- The coverage check of `cold.Reader.Search` for `[lo, hi]` at `now`, with
committed coverage ending at `cov`: the query fails when it needs an
interval below the required end that Cold has not archived. -/
def gatewayAccepts (c : Contract) (cov lo hi now : Int) : Prop :=
  ¬ (cov < requiredEnd c now ∧ cov ≤ hi ∧ lo < requiredEnd c now)

/-- Truncating division never rounds down by a whole divisor: the required
end trails `now - H` by less than one window, whatever the sign. -/
theorem requiredEnd_gt (c : Contract) (now : Int) :
    now - c.H - c.w < requiredEnd c now := by
  unfold requiredEnd
  have hdecomp := Int.tdiv_mul_add_tmod (now - c.H) c.w
  have hmod := Int.tmod_lt_of_pos (now - c.H) c.window_pos
  generalize Int.tdiv (now - c.H) c.w * c.w = q at *
  omega

/-- Once `now ≥ H`, the required end is at most `now - H`: the gateway never
requires a window that `validateTime` would still refuse as too young. -/
theorem requiredEnd_le (c : Contract) (now : Int) (h : c.H ≤ now) :
    requiredEnd c now ≤ now - c.H := by
  unfold requiredEnd
  have hdecomp := Int.tdiv_mul_add_tmod (now - c.H) c.w
  have hmod := Int.tmod_nonneg c.w (show 0 ≤ now - c.H by omega)
  generalize Int.tdiv (now - c.H) c.w * c.w = q at *
  omega

/-- A window `[start, start + w)` can be archived exactly while
`start + w + H ≤ now < start + R - s`. -/
theorem timeValid_iff (c : Contract) (start now : Int) :
    timeValid c start (start + c.w) now ↔
      start + c.w + c.H ≤ now ∧ now < start + c.R - c.s := by
  unfold timeValid
  omega

/-- That interval is never empty: every window has
`R - H - s - w > 0` milliseconds in which it can be archived. -/
theorem archivable_interval_nonempty (c : Contract) (start : Int) :
    ∃ now, timeValid c start (start + c.w) now := by
  refine ⟨start + c.w + c.H, ?_⟩
  rw [timeValid_iff]
  have := c.retention_headroom
  omega

/-- An attempt checks `validateTime` when it starts exporting at `x1` and
again at `x2 ≥ x1` before publication. Then every sample of the window that
ingress ever accepts (at a time `a ≤ t + L`) was accepted before the export
began, and Hot still retained it at every instant of the export: the
published window is complete. -/
theorem export_complete (c : Contract) (start x1 x2 t a : Int)
    (h1 : timeValid c start (start + c.w) x1)
    (h2 : timeValid c start (start + c.w) x2)
    (ht : start ≤ t ∧ t < start + c.w)
    (hlate : a ≤ t + c.L) :
    a < x1 ∧ ∀ x, x1 ≤ x → x ≤ x2 → x - c.R < t := by
  unfold timeValid at h1 h2
  have := c.lateness_lt_age
  have := c.safety_pos
  refine ⟨by omega, fun x _ _ => by omega⟩

/-- A query at `now` for `[lo, hi]` that the gateway accepts misses no
accepted sample at or after the coverage start: each one lies below the
coverage end, where Cold serves it, or is younger than `R`, where Hot still
holds it. -/
theorem query_complete (c : Contract) (cov lo hi now t : Int)
    (hacc : gatewayAccepts c cov lo hi now)
    (ht : lo ≤ t ∧ t ≤ hi) :
    t < cov ∨ now - c.R < t := by
  unfold gatewayAccepts at hacc
  have hreq := requiredEnd_gt c now
  have := c.retention_headroom
  have := c.safety_pos
  generalize requiredEnd c now = req at *
  omega

/-- The headroom condition is needed: with `R = H + w` (so `R > H + s + w`
fails), a query at `now = 25` with coverage ending at the required end
`10` passes the gateway, yet a sample at `10` is neither below the coverage
end nor younger than `R`: Hot has already dropped it. -/
example :
    let H := 10; let w := 10; let R := 15
    let now := 25; let cov := Int.tdiv (now - H) w * w; let t := 10
    ¬ (cov < Int.tdiv (now - H) w * w ∧ cov ≤ t ∧ t < Int.tdiv (now - H) w * w) ∧
      ¬ (t < cov ∨ now - R < t) := by
  decide

end VmtierTiming
