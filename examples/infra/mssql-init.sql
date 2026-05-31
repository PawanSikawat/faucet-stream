-- Databases + sample tables for the faucet-stream MSSQL examples.
-- Run automatically by the `mssql-init` sidecar in docker-compose.yml.

IF DB_ID('sales') IS NULL CREATE DATABASE sales;
IF DB_ID('analytics') IS NULL CREATE DATABASE analytics;
IF DB_ID('raw') IS NULL CREATE DATABASE raw;
GO

-- sales.dbo.users — source for mssql_to_jsonl.yaml (incremental on updated_at).
USE sales;
IF OBJECT_ID('dbo.users', 'U') IS NULL
  CREATE TABLE dbo.users (
    id INT PRIMARY KEY,
    email NVARCHAR(255),
    updated_at DATETIME2
  );
IF NOT EXISTS (SELECT 1 FROM dbo.users)
  INSERT INTO dbo.users (id, email, updated_at) VALUES
    (1, 'alice@example.com', '2024-01-01T00:00:00'),
    (2, 'bob@example.com',   '2024-06-01T12:00:00'),
    (3, 'carol@example.com', '2024-12-15T08:30:00');
GO

-- analytics.dbo.events — target for kafka_to_mssql.yaml (auto_columns). Adjust
-- the columns to match your Kafka message keys.
USE analytics;
IF OBJECT_ID('dbo.events', 'U') IS NULL
  CREATE TABLE dbo.events (
    id INT,
    name NVARCHAR(100),
    payload NVARCHAR(MAX)
  );
GO

-- raw.dbo.products_raw is auto-created by rest_to_mssql.yaml (create_table: true),
-- so only the `raw` database needs to exist here.
