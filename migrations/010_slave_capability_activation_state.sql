-- Persist runtime-neutral activation desired/actual state on each Slave.
-- The JSON row contains the validated package payload needed for restart
-- reconciliation; it is deliberately role-local and never projected into
-- Observer identity/digest data.
ALTER TABLE slave_replica
    ADD COLUMN IF NOT EXISTS capability_activations JSONB NOT NULL DEFAULT '[]'::jsonb;
