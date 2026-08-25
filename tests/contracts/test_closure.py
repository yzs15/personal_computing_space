from loom_v2.contracts.constraints import Constraint
from loom_v2.contracts.terms import TypedTerm
from loom_v2.contracts.types import ComputeApplication, ComputeRequirement, ComputeSpec, DataApplication, TaskClosure, TypedHole


def test_compute_requirement_uses_constraint_ref_as_single_source():
    constraint = Constraint(
        subject=["ComputeSpec"],
        predicate={"op": "le", "field": "cpu_seconds", "value": 60},
        source="Requester",
        fate="preserve",
    )
    requirement = ComputeRequirement(
        key="loom.compute.budget.v1",
        value={"cpu_seconds": 60},
        view="systems",
        constraint_ref=constraint.ref(),
    )
    closure = TaskClosure.minimal(
        compute=ComputeSpec(
            requirements=[requirement],
            typed_holes=[TypedHole(hole_id="h_compute")],
        ),
        constraints=[constraint],
    )
    assert closure.compute.requirements[0].constraint_ref.constraint_id == constraint.constraint_id


def test_digest_is_stable_for_key_order():
    first = TaskClosure.minimal(metadata={"b": 2, "a": 1})
    second = TaskClosure.minimal(metadata={"a": 1, "b": 2})
    assert first.canonical_digest() == second.canonical_digest()


def test_terms_are_attached_to_each_semantic_view():
    term = TypedTerm(kind="loom.compute.precision.v1", schema_ref="loom.compute.precision/1", value={"epsilon": 0.01})
    closure = TaskClosure(
        data=DataApplication(terms=[term]),
        compute_application=ComputeApplication(terms=[term]),
    )
    assert closure.data.terms[0].kind == term.kind
    assert closure.compute_application.terms[0].kind == term.kind
