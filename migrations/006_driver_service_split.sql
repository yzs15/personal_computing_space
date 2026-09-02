CREATE TABLE IF NOT EXISTS runtime_agents (
    workspace_id VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL,
    agent_id VARCHAR(128) NOT NULL,
    instance_id VARCHAR(128) NOT NULL,
    endpoint_url VARCHAR(512) NOT NULL,
    protocol_version VARCHAR(64) NOT NULL,
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    epoch BIGINT NOT NULL DEFAULT 0,
    lease_id_hash VARCHAR(128) NOT NULL DEFAULT '',
    lease_state VARCHAR(32) NOT NULL DEFAULT 'expired',
    last_seen_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (workspace_id, role, agent_id, instance_id)
);

CREATE TABLE IF NOT EXISTS driver_threads (
    workspace_id VARCHAR(128) NOT NULL,
    conversation_ref VARCHAR(256) NOT NULL,
    thread_id VARCHAR(256) NOT NULL,
    model VARCHAR(256) NOT NULL,
    workspace_root VARCHAR(512) NOT NULL,
    last_turn_id VARCHAR(256),
    turn_state VARCHAR(32) NOT NULL DEFAULT 'idle',
    active_request_id VARCHAR(256),
    driver_epoch BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (workspace_id, conversation_ref),
    UNIQUE (workspace_id, conversation_ref),
    UNIQUE (workspace_id, thread_id)
);

CREATE TABLE IF NOT EXISTS driver_requests (
    driver_id VARCHAR(128) NOT NULL,
    request_id VARCHAR(256) NOT NULL,
    command VARCHAR(128) NOT NULL,
    response JSONB NOT NULL,
    PRIMARY KEY (driver_id, request_id)
);

CREATE INDEX IF NOT EXISTS idx_runtime_agents_workspace_role_agent
    ON runtime_agents (workspace_id, role, agent_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_agents_active_agent
    ON runtime_agents (workspace_id, role, agent_id)
    WHERE lease_state = 'active';
CREATE UNIQUE INDEX IF NOT EXISTS uq_runtime_agents_active_driver
    ON runtime_agents (workspace_id)
    WHERE role = 'driver' AND lease_state = 'active';
CREATE INDEX IF NOT EXISTS idx_driver_threads_workspace_conversation
    ON driver_threads (workspace_id, conversation_ref);
CREATE INDEX IF NOT EXISTS idx_driver_requests_driver_request
    ON driver_requests (driver_id, request_id);
