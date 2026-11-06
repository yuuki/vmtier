# Move a stopped SQLite catalog to PostgreSQL

`catalog-import-postgres` copies metadata into a fresh PostgreSQL catalog under a
new identity. It preserves the archive contract, blob locator, per-stream coverage,
window generations and digests, ordered pack descriptors, and explicit empty
windows. Existing S3 objects are neither downloaded nor rewritten by the importer.

## Prepare and stop the source

Provision a dedicated PostgreSQL database/schema and prepare the
[PostgreSQL configuration](postgres-catalog.md). Preserve the SQLite configuration's
tenants, source IDs and URLs, coverage origin, timing policy, and exact S3 locator.
Choose a different `catalog.id` and supply the DSN through `catalog.dsn_env`.
Set a replica budget that includes verification/cutover traffic.

Stop the SQLite service and disable its supervisor/restart mechanism. Take an
offline backup and keep the source directory. Plan enough remaining Hot retention
for the complete import, verification, cutover, and archive catch-up. The importer
acquires the same lifetime directory lock as SQLite `serve`; a running owner makes
the command fail. It retains ownership through source validation, import, and
destination verification.

Do **not** run PostgreSQL `catalog-init` first: the import creates schema and state
atomically and refuses an existing destination.

```sh
.bin/vmtier catalog-import-postgres \
  -config postgres.json \
  -from .state/sqlite-catalog \
  -source-id OLD-SQLITE-ID \
  -timeout 30m
.bin/vmtier inspect -config postgres.json
.bin/vmtier catalog-inspect -backend postgres -config postgres.json \
  -tenant 1:2 -start START-MS -end END-MS
```

The timeout defaults to 30 minutes and must be positive and at most 24 hours.
The configuration's ID identifies the new PostgreSQL authority; `-source-id`
pins the old authority. Contract, identity, and locator mismatches fail before
destination mutation. PostgreSQL runtime requires shared S3, so the CLI does not
silently reinterpret local filesystem blobs as shared storage.

## Atomicity and verification

The importer checks the source catalog's contiguous streams, publication digests,
pack descriptors, and generations while holding the source lock. It streams
windows from a SQLite snapshot into one PostgreSQL transaction. The destination
is read back to compare identity/contract/coverage, row counts, and a digest over
all ordered window references before commit. Memory is bounded to one publication.
The lease starts unowned, ready for the new service to acquire a fresh token.

A validation or transaction failure leaves no initialized partial destination.
An interrupted commit response is ambiguous: inspect the new identity before
retrying. If the import committed, a repeated import refuses to overwrite it.
Never drop a destination automatically because a client timed out.

This validates metadata equivalence, not physical object availability or source
completeness. Run `verify -config postgres.json` for a payload scrub within the
allocated S3 capacity, and retain the source backup. Importing old metadata cannot
recover objects that are missing or Hot history that has already expired.

## Cut over

1. Keep the old SQLite owner disabled and fenced from restarting.
2. Start PostgreSQL-backed candidates and wait for `/ready`. Their leased archiver
   resumes from the imported coverage while Hot still retains the missing suffix.
3. Compare Cold-only and Hot/Cold-boundary queries with the reference/source
   deployment. The HA harness's migration mode exercises this transition locally.
4. Switch the single logical Cold endpoint to the PostgreSQL gateways only after
   validation. Keep old metadata and backups for investigation/recovery.

Before any PostgreSQL publication, abandoning the import can return to the stopped
SQLite authority only after all PostgreSQL writers are isolated. Once PostgreSQL
advances, restarting the old SQLite catalog would restore stale authority. Continue
with PostgreSQL recovery or perform a separately audited metadata reconciliation;
there is no automatic reverse migration. The source lock ends when the import
command exits and is not a permanent fence against an old supervisor.
