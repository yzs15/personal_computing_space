from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from loom_v2.digest import digest_json


class UnknownRequiredTerm(ValueError):
    """Raised when a required term is not present in the registry."""


class TypedTerm(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    term_id: str = ""
    kind: str
    schema_ref: str
    value: Any
    criticality: Literal["required", "advisory"] = "required"
    constraint_ref: Any | None = None
    refinement_relation_ref: str | None = None
    provenance: list[dict[str, Any]] = Field(default_factory=list)

    @field_validator("term_id", mode="before")
    @classmethod
    def default_term_id(cls, value: str | None) -> str:
        # The Python object id is process-local and is not a content identity.
        # Callers should provide a stable term_id; this fallback is only a
        # deterministic marker for legacy callers.
        return value or "term-anonymous"


class TermSupport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str
    schema_ref: str
    support: set[str] = Field(default_factory=set)
    execution_stages: set[str] = Field(default_factory=set)
    evidence_kinds: set[str] = Field(default_factory=set)
    support_version: str = "1"

    def can(self, operation: str, stage: str) -> bool:
        return operation in self.support and stage in self.execution_stages


class VocabularyRegistryEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str
    schema_ref: str
    value_schema: str
    refinement_relation: str = "opaque"
    propagation_rules: dict[str, Any] = Field(default_factory=dict)
    evidence_kinds: list[str] = Field(default_factory=list)
    compatibility: dict[str, Any] = Field(default_factory=dict)
    registry_version: str = "1"


class VocabularyRegistry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    registry_id: str = "loom-builtin"
    version: str = "1"
    entries: dict[str, VocabularyRegistryEntry] = Field(default_factory=dict)
    digest: str = ""
    provenance: list[dict[str, Any]] = Field(default_factory=list)

    def model_post_init(self, __context: Any) -> None:
        if not self.digest:
            payload = {key: value.model_dump(mode="json") for key, value in sorted(self.entries.items())}
            self.digest = digest_json(payload, domain="loom/vocabulary-registry/v1")

    def validate(self, term: TypedTerm) -> TypedTerm:
        entry = self.entries.get(term.kind)
        if entry is None:
            if term.criticality == "required":
                raise UnknownRequiredTerm(term.kind)
            return term
        if entry.schema_ref != term.schema_ref:
            raise ValueError(f"schema_incompatible:{term.kind}")
        if term.kind.endswith("precision.v1"):
            if not isinstance(term.value, dict) or not isinstance(term.value.get("epsilon"), (int, float)) or term.value["epsilon"] <= 0:
                raise ValueError("invalid_precision_value")
        return term

    def round_trip(self, term: TypedTerm) -> TypedTerm:
        return term if term.criticality == "advisory" else self.validate(term)


def builtin_registry() -> VocabularyRegistry:
    names = {
        "loom.compute.capability.v1": "loom.compute.capability/1",
        "loom.compute.precision.v1": "loom.compute.precision/1",
        "loom.compute.parallelism.v1": "loom.compute.parallelism/1",
        "loom.compute.budget.v1": "loom.compute.budget/1",
        "loom.data.locality.v1": "loom.data.locality/1",
        "loom.compute.network.v1": "loom.compute.network/1",
        "loom.compute.deadline.v1": "loom.compute.deadline/1",
    }
    entries = {
        kind: VocabularyRegistryEntry(kind=kind, schema_ref=schema, value_schema=schema, refinement_relation="monotonic")
        for kind, schema in names.items()
    }
    return VocabularyRegistry(entries=entries)
