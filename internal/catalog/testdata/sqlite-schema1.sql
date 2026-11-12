CREATE TABLE catalog_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 schema_version INTEGER NOT NULL,
 catalog_id TEXT NOT NULL,
 vm_version TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK (generation >= 0),
 contract_json TEXT NOT NULL,
 blob_json TEXT NOT NULL
);
CREATE TABLE coverage (
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 end_ms INTEGER NOT NULL CHECK (end_ms >= 0),
 PRIMARY KEY (tenant, source_id)
) WITHOUT ROWID;
CREATE TABLE windows (
 id INTEGER PRIMARY KEY,
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 start_ms INTEGER NOT NULL CHECK (start_ms >= 0),
 end_ms INTEGER NOT NULL CHECK (end_ms > start_ms),
 generation INTEGER NOT NULL UNIQUE CHECK (generation > 0),
 pack_count INTEGER NOT NULL CHECK (pack_count >= 0 AND pack_count <= 1024),
 digest TEXT NOT NULL,
 UNIQUE (tenant, source_id, start_ms),
 FOREIGN KEY (tenant, source_id) REFERENCES coverage (tenant, source_id)
);
CREATE TABLE packs (
 window_id INTEGER NOT NULL REFERENCES windows(id),
 ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
 data_key TEXT NOT NULL UNIQUE,
 index_key TEXT NOT NULL UNIQUE,
 data_sha256 TEXT NOT NULL,
 index_sha256 TEXT NOT NULL,
 data_bytes INTEGER NOT NULL CHECK (data_bytes >= 0 AND data_bytes <= 67108864),
 index_bytes INTEGER NOT NULL CHECK (index_bytes > 0 AND index_bytes <= 16777216),
 PRIMARY KEY (window_id, ordinal)
) WITHOUT ROWID;
