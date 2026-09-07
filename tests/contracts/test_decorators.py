from loom_v2.contracts.constraints import ConstraintSpec
from loom_v2.contracts.decorators import attach_constraint, task_closure


def test_decorators_materialize_explicit_metadata_without_calling_function():
    calls = []

    @task_closure(goal="run test code", operation_ref="loom://test_double")
    @attach_constraint(
        ConstraintSpec(
            subject=["ComputeSpec"],
            predicate={"op": "le", "field": "cpu_seconds", "value": 1},
            source="Requester",
            fate="preserve",
        )
    )
    def task():
        calls.append("executed")

    contract = task.materialize_contract()
    assert contract.goal == "run test code"
    assert calls == []
    assert len(contract.declared_constraints) == 1
