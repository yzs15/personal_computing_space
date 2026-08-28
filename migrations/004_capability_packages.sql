ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS capability_packages JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS capability_activations JSONB NOT NULL DEFAULT '[]'::jsonb;
