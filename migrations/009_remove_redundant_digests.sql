-- Remove the unused replica digest.  Replica state is authoritative; it is
-- not a content snapshot and therefore must not carry a misleading digest.
ALTER TABLE slave_replica DROP COLUMN IF EXISTS digest;
ALTER TABLE message_receipts DROP COLUMN IF EXISTS payload_digest;
