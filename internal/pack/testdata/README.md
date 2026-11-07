`native-v1.128.0.hex` is an independent small golden native-export stream for
VictoriaMetrics OSS v1.128.0-cluster, commit
`9a009c491da4c17531893ab4acb6a5f1b5e0eaf2`.

It represents logical interval `[0, 100)` milliseconds, metric `x`, and samples
`(0, 2)` and `(50, 2)`. The tenant is intentionally absent from the stream.
The fixture was constructed from the pinned `ExportNativeHandler` framing and
checked byte-for-byte against the public pinned `Block.MarshalPortable` and
`MetricName.MarshalNoAccountIDProjectID` APIs. It is not a captured HTTP response.
Stock-process HTTP/RPC compatibility is tested separately by `scripts/e2e.py`.

Codec upgrades must retain this decoder or explicitly reject the old revision;
regenerating this fixture to make an incompatible decoder pass is not a migration.


`native-v1.155.0.hex` is a captured native HTTP response from unmodified
VictoriaMetrics OSS v1.155.0-cluster, commit
`cab3d5fbeeedd5d0374c1dabde93939af4c2499c`, built with its own vendored dependencies.
On 2026-11-06, a disposable one-node cluster imported this JSON for tenant `42:7`:

```json
{"metric":{"__name__":"fixture_155","job":"codec"},"values":[2,3.5],"timestamps":[1793923200010,1793923200050]}
```

After the write buffer and storage were flushed, `/select/42:7/prometheus/api/v1/export/native`
was requested with `match[]=fixture_155`, `start=1793923200.0`, and
`end=1793923200.099`. The 68-byte body is stored as hex without modification.
The test checks the independently specified labels, timestamps, decimal values,
and restoration of tenant identity. The earlier v1.128.0 fixture remains unchanged
and is still decoded by the current codec test. That byte compatibility does not
permit opening old pack indexes or catalogs: their release/revision must still
match the supported deployment.
