# Loom role-local Alembic migrations

The first public release starts from one squashed `001_release_baseline`
revision. It creates only the schema owned by the selected role: Observer or
Slave. The runtime supplies the role and an existing SQLAlchemy connection
through `loom_v2.db.migrations`.

Application startup and `scripts/migrate.sh` both call this adapter. Future
schema changes must add a new Alembic revision; the release baseline is
immutable.
