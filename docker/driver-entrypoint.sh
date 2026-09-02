#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
  exec "$@"
fi

read_secret() {
  path="$1"
  if [ ! -r "$path" ]; then
    echo "missing secret: $path" >&2
    exit 1
  fi
  value=$(cat "$path")
  if [ -z "$value" ]; then
    echo "empty secret: $path" >&2
    exit 1
  fi
  printf '%s' "$value"
}

internal_api_secret=$(read_secret /run/secrets/internal_api_secret)
codex_api_key_file="${LOOM_CODEX_API_KEY_FILE:-/run/secrets/codex_api_key}"
codex_api_key=""
if [ -r "$codex_api_key_file" ]; then
  codex_api_key=$(read_secret "$codex_api_key_file")
fi
codex_home="${CODEX_HOME:-/var/lib/loom/codex}"
mkdir -p "$codex_home"
chmod 700 "$codex_home"
codex_api_key_env="${LOOM_CODEX_API_KEY_ENV:-OPENAI_API_KEY}"
codex_env_key_line=""
if [ -n "$codex_api_key" ]; then
  codex_env_key_line="env_key = \"$codex_api_key_env\""
fi
model_catalog_json="${LOOM_CODEX_MODEL_CATALOG_JSON:-}"
catalog_line=""
if [ -n "$model_catalog_json" ]; then
  catalog_line="model_catalog_json = \"$model_catalog_json\""
fi
cat > "$codex_home/config.toml" <<EOF
model_provider = "${LOOM_CODEX_PROVIDER:-proxy}"
model = "${LOOM_CODEX_MODEL:-deepseek-v4-flash}"
model_reasoning_effort = "${LOOM_CODEX_MODEL_REASONING_EFFORT:-xhigh}"
approvals_reviewer = "${LOOM_CODEX_APPROVALS_REVIEWER:-guardian_subagent}"
sandbox_mode = "${LOOM_CODEX_SANDBOX_MODE:-danger-full-access}"
$catalog_line

[model_providers.${LOOM_CODEX_PROVIDER:-proxy}]
name = "${LOOM_CODEX_PROVIDER:-proxy}"
base_url = "${LOOM_CODEX_BASE_URL:-http://host.docker.internal:8787}"
$codex_env_key_line
wire_api = "${LOOM_CODEX_WIRE_API:-responses}"
EOF
chmod 600 "$codex_home/config.toml"
export LOOM_INTERNAL_API_SECRET="$internal_api_secret"
if [ -n "$codex_api_key" ]; then
  export "$codex_api_key_env=$codex_api_key"
else
  unset "$codex_api_key_env" || true
fi
exec uvicorn loom_v2.driver.app:app --host 0.0.0.0 --port 8090
