-- Idempotent: safe to run any number of times.
INSERT INTO greetings (message)
SELECT 'Hello from the seed data'
WHERE NOT EXISTS (SELECT 1 FROM greetings);

-- Demo project and API key. The key is public: never seed a production database.
INSERT INTO projects (name) VALUES ('demo') ON CONFLICT (name) DO NOTHING;
INSERT INTO api_keys (project_id, prefix, key_hash)
SELECT id, 'obs_demo_loc', sha256('obs_demo_local_dev_only'::bytea) FROM projects WHERE name = 'demo'
ON CONFLICT (key_hash) DO NOTHING;
