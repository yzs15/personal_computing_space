ALTER TABLE runs
    ADD COLUMN IF NOT EXISTS closure_contract JSONB;
