SET lock_timeout = '3s';

CREATE TABLE projects (
  id bigserial PRIMARY KEY,
  name text NOT NULL UNIQUE,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Only the SHA-256 of a key is stored. `prefix` (e.g. "obs_ab12cd34") is shown in the UI
-- so people can tell keys apart; the full key is shown once, at creation.
CREATE TABLE api_keys (
  id bigserial PRIMARY KEY,
  project_id bigint NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  prefix text NOT NULL,
  key_hash bytea NOT NULL UNIQUE CHECK (length(key_hash) = 32),
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz
);
CREATE INDEX api_keys_project_id_idx ON api_keys (project_id);
