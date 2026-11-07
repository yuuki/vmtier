## What and why

<!-- The concrete problem, the resulting behavior, and any limitation. -->

## How tested

<!-- Commands run (make check, test-integration, e2e suites, fuzzing) and results. -->

## Checklist

- [ ] `make check` passes (after `make bootstrap`).
- [ ] Behavior changes have tests; archive, publication, query, RPC or S3 changes ran the relevant stock-VM differential, recovery or migration suites.
- [ ] Docs, examples and `docs/evidence.md` are updated where commands, defaults, metrics or claims changed.
- [ ] Public contracts (VM pin, pack/catalog formats, object keys, CLI flags, config keys, metrics, RPCs) are unchanged, or the change is called out above.
- [ ] New linked modules have a reviewed entry in `release/licenses.json` and `THIRD_PARTY_NOTICES.md`.
- [ ] AI-assisted changes were reviewed by a human.
