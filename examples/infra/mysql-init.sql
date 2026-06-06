-- Seed data + replication user for the faucet-stream MySQL examples.
-- Runs automatically on first start of the mysql service in docker-compose.yml.

-- A simple table the query and CDC examples read from.
CREATE TABLE IF NOT EXISTS users (
    id   INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    city VARCHAR(255)
);

INSERT INTO users (name, city) VALUES
    ('Ada',   'London'),
    ('Grace', 'New York'),
    ('Linus', 'Helsinki');

-- Replication user for the mysql-cdc source (matches cli/examples/mysql_cdc_to_jsonl.yaml).
-- REPLICATION SLAVE: allows the user to request binlog events.
-- REPLICATION CLIENT: allows SHOW MASTER STATUS and SHOW SLAVE STATUS.
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
