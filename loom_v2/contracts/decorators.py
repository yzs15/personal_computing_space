from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from .constraints import Constraint, ConstraintSpec
from .types import ClosureContract, ComputeSpec, DataApplication, ProgramApplication, TaskClosure


def attach_constraint(spec: ConstraintSpec) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    constraint = Constraint.model_validate(spec.model_dump())

    def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
        constraints = list(getattr(function, "_loom_constraints", []))
        constraints.append(constraint)
        setattr(function, "_loom_constraints", constraints)
        return function

    return decorator


def task_closure(
    *,
    goal: str,
    operation_ref: str = "",
    data: DataApplication | None = None,
    compute: ComputeSpec | None = None,
    terms: list[Any] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
        constraints = list(getattr(function, "_loom_constraints", []))
        closure = TaskClosure(
            data=data or DataApplication(),
            program=ProgramApplication(operation_ref=operation_ref),
            compute=compute or ComputeSpec(operation_ref=operation_ref),
            terms=terms or [],
            constraints=constraints,
        )
        contract = ClosureContract(closure_id=f"closure-{function.__name__}", goal=goal, declared_constraints=constraints, body=closure)

        @wraps(function)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return function(*args, **kwargs)

        def materialize_contract() -> ClosureContract:
            return contract.model_copy(deep=True)

        setattr(wrapped, "materialize_contract", materialize_contract)
        return wrapped

    return decorator
