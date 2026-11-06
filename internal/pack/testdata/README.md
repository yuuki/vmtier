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
