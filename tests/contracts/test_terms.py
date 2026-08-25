import pytest

from loom_v2.contracts.terms import TermSupport, UnknownRequiredTerm, builtin_registry
from loom_v2.contracts.types import TypedTerm


def test_builtin_registry_validates_precision_term():
    registry = builtin_registry()
    term = TypedTerm(
        kind="loom.compute.precision.v1",
        schema_ref="loom.compute.precision/1",
        value={"epsilon": 0.01},
        criticality="required",
    )
    assert registry.validate(term).kind == term.kind


def test_unknown_required_term_is_rejected_but_advisory_round_trips():
    registry = builtin_registry()
    required = TypedTerm(
        kind="vendor.new.v1",
        schema_ref="vendor.new/1",
        value={"x": 1},
        criticality="required",
    )
    advisory = required.model_copy(update={"criticality": "advisory"})
    with pytest.raises(UnknownRequiredTerm):
        registry.validate(required)
    assert registry.round_trip(advisory) == advisory


def test_slave_support_is_stage_specific():
    support = TermSupport(
        kind="loom.compute.precision.v1",
        schema_ref="loom.compute.precision/1",
        support={"parse", "preserve", "validate"},
        execution_stages={"commit"},
    )
    assert support.can("validate", "commit")
    assert not support.can("enforce", "execute")
