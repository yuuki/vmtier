CREATE TABLE %[1]s.catalog_state (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 schema_version INTEGER NOT NULL,
 catalog_id TEXT NOT NULL,
 vm_version TEXT NOT NULL,
 generation BIGINT NOT NULL CHECK (generation >= 0),
 contract_json TEXT NOT NULL CHECK (octet_length(contract_json) <= 1048576),
 blob_json TEXT NOT NULL CHECK (octet_length(blob_json) <= 16384)
);
CREATE TABLE %[1]s.coverage (
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 end_ms BIGINT NOT NULL CHECK (end_ms >= 0),
 PRIMARY KEY (tenant, source_id)
);
CREATE TABLE %[1]s.windows (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 tenant TEXT NOT NULL,
 source_id TEXT NOT NULL,
 start_ms BIGINT NOT NULL CHECK (start_ms >= 0),
 end_ms BIGINT NOT NULL CHECK (end_ms > start_ms),
 generation BIGINT NOT NULL UNIQUE CHECK (generation > 0),
 pack_count INTEGER NOT NULL CHECK (pack_count >= 0 AND pack_count <= 1024),
 digest TEXT NOT NULL CHECK (octet_length(digest) = 64),
 UNIQUE (tenant, source_id, start_ms),
 FOREIGN KEY (tenant, source_id) REFERENCES %[1]s.coverage (tenant, source_id)
);
CREATE TABLE %[1]s.object_keys (
 object_key TEXT PRIMARY KEY CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
 window_id BIGINT NOT NULL REFERENCES %[1]s.windows(id),
 UNIQUE (object_key, window_id)
);
CREATE TABLE %[1]s.packs (
 window_id BIGINT NOT NULL REFERENCES %[1]s.windows(id),
 ordinal INTEGER NOT NULL CHECK (ordinal >= 0 AND ordinal < 1024),
 data_key TEXT NOT NULL UNIQUE,
 index_key TEXT NOT NULL UNIQUE,
 data_sha256 TEXT NOT NULL CHECK (octet_length(data_sha256) = 64),
 index_sha256 TEXT NOT NULL CHECK (octet_length(index_sha256) = 64),
 data_bytes BIGINT NOT NULL CHECK (data_bytes >= 0 AND data_bytes <= 67108864),
 index_bytes BIGINT NOT NULL CHECK (index_bytes > 0 AND index_bytes <= 16777216),
 PRIMARY KEY (window_id, ordinal),
 CHECK (data_key <> index_key),
 FOREIGN KEY (data_key, window_id) REFERENCES %[1]s.object_keys(object_key, window_id),
 FOREIGN KEY (index_key, window_id) REFERENCES %[1]s.object_keys(object_key, window_id)
);
CREATE TABLE %[1]s.archive_lease (
 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
 owner TEXT NOT NULL CHECK (octet_length(owner) <= 128),
 token BIGINT NOT NULL CHECK (token >= 0),
 expires_at TIMESTAMPTZ NOT NULL,
 CHECK (owner = '' OR token > 0)
);
INSERT INTO %[1]s.archive_lease VALUES (1, '', 0, 'epoch');
