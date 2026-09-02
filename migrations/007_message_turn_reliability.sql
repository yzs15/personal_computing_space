CREATE TABLE IF NOT EXISTS message_receipts (
    workspace_id VARCHAR(128) NOT NULL,
    request_id VARCHAR(256) NOT NULL,
    conversation_ref VARCHAR(256) NOT NULL,
    prompt TEXT NOT NULL,
    payload_digest VARCHAR(64) NOT NULL,
    state VARCHAR(32) NOT NULL,
    run_id VARCHAR(128),
    assistant_text TEXT,
    outcome JSONB,
    claim_token VARCHAR(128),
    claim_expires_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workspace_id, request_id)
);
CREATE INDEX IF NOT EXISTS idx_message_receipts_conversation
    ON message_receipts (workspace_id, conversation_ref, created_at);
CREATE INDEX IF NOT EXISTS idx_message_receipts_dispatch
    ON message_receipts (state, next_attempt_at);
