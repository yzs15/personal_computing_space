CREATE TABLE IF NOT EXISTS runs (
    run_id VARCHAR(128) PRIMARY KEY,
    task_ref VARCHAR(256) NOT NULL,
    goal VARCHAR(2048) NOT NULL,
    allow_reassignment BOOLEAN NOT NULL DEFAULT FALSE,
    draft JSONB NOT NULL,
    committed JSONB,
    execution_id VARCHAR(128),
    execution_epoch INTEGER NOT NULL DEFAULT 1,
    state VARCHAR(64) NOT NULL,
    attempts JSONB NOT NULL DEFAULT '[]'::jsonb,
    events JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS idempotency (
    operation_id VARCHAR(256) PRIMARY KEY,
    receipt JSONB NOT NULL
);
