# Historical v1 evidence

These files document validation on 2026-11-06 of the previous architecture, which
stored the catalog in object storage and used conditional writes. They are
historical evidence only and do not validate the current independent catalog
authority or ordinary-PUT blob store.

The original JSON results are preserved byte-for-byte. The accompanying validation
and testing documents have been translated into English and their relative links
have been repaired; they are not byte-for-byte copies of the original prose.
The current checkout provides SQLite `serve` and the retained JSON v2 commands;
commands in the historical testing document do not reproduce v1 with current sources.

See the [validation index](../../README.md#validation-records) for SQLite and JSON
v2 evidence, and the [current architecture](../../architecture.md) for implemented behavior.
