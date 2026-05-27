-- Seed data + logical-replication publication for the faucet-stream examples.
-- Runs automatically on first start of the postgres service in docker-compose.yml.

-- A simple table the query and CDC examples read from.
CREATE TABLE IF NOT EXISTS users (
    id   integer PRIMARY KEY,
    name text NOT NULL,
    city text
);

INSERT INTO users (id, name, city) VALUES
    (1, 'Ada',   'London'),
    (2, 'Grace', 'New York'),
    (3, 'Linus', 'Helsinki')
ON CONFLICT (id) DO NOTHING;

-- Publication used by the postgres-cdc source (slot is created by the connector
-- when create_slot_if_missing: true). Matches cli/examples/postgres_cdc_to_jsonl.yaml.
CREATE PUBLICATION faucet_pub FOR TABLE users;
