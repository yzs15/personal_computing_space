from loom_v2.contracts.constraints import Constraint
from loom_v2.contracts.types import ComputeRequirement, ComputeSpec, TaskClosure, TypedHole


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
