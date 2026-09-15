# Operator package contracts

Place declarative `*.json` package contract files in this directory. Each file must contain exactly:

```json
{
  "package_type": "example",
  "execution": {"kind": "vendor:runtime", "version": "1"},
  "schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["package_type", "execution", "capability_exports", "body"],
    "properties": {
      "package_type": {"const": "example"},
      "execution": {
        "type": "object",
        "required": ["kind", "version"],
        "properties": {
          "kind": {"const": "vendor:runtime"},
          "version": {"const": "1"}
        },
        "additionalProperties": false
      },
      "capability_exports": {"type": "array", "minItems": 1},
      "body": {"type": "object"}
    },
    "additionalProperties": false
  }
}
```

The schema must be a complete manifest schema; production contracts should
strictly define every export and body field. Observer, Driver, and Slave load
the same read-only directory at process start. Built-in contracts do not need
files here.
