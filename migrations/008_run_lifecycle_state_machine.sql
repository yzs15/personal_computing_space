-- Run lifecycle migration.  This file is intentionally idempotent so the
-- existing deployment script can safely apply it on every startup.

-- Older rows used decision_required for both repair and attestation.  A raw
-- failed terminal state is authoritative for repair; all other legacy
-- decision_required outcomes are treated as attestation.  SQLAlchemy creates
-- ``outcome`` as PostgreSQL JSON, so all merge operations cast through JSONB
-- and then back to JSON before assignment.
UPDATE runs
SET state = 'awaiting_decision',
    outcome = (
        COALESCE(outcome::jsonb, '{}'::jsonb)
        || jsonb_build_object(
        'disposition', CASE
            WHEN COALESCE(outcome->>'disposition', '') <> ''
                THEN outcome->>'disposition'
            ELSE 'awaiting_decision'
        END,
        'decision', CASE
            WHEN COALESCE(outcome->>'decision', '') <> ''
                THEN outcome->>'decision'
            WHEN outcome->>'terminal_state' = 'failed'
                THEN 'repair'
            ELSE 'attestation'
        END
        )
    )::text::json
WHERE state = 'decision_required';

ALTER TABLE message_receipts
    DROP COLUMN IF EXISTS claim_expires_at;
