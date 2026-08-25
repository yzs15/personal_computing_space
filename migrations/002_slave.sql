-- Apply only to a Slave's role-local PostgreSQL database.
CREATE TABLE IF NOT EXISTS slave_attempts (
    attempt_id VARCHAR(128) PRIMARY KEY,
    slave_id VARCHAR(128) NOT NULL,
    workspace_id VARCHAR(128) NOT NULL,
    operation VARCHAR(128) NOT NULL,
    payload JSONB NOT NULL,
    state VARCHAR(64) NOT NULL,
    result JSONB
);

CREATE TABLE IF NOT EXISTS slave_replica (
    slave_id VARCHAR(128) PRIMARY KEY,
    workspace_id VARCHAR(128) NOT NULL,
    state VARCHAR(32) NOT NULL,
    digest VARCHAR(256) NOT NULL DEFAULT ''
);
