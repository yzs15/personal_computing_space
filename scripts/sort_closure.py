from __future__ import annotations

import asyncio

from loom_v2.contracts.terms import TypedTerm
from loom_v2.contracts.types import (
    ComputeApplication,
    ComputeSpec,
    DataApplication,
    ProgramApplication,
    ResourceRef,
    TaskClosure,
    TypedHole,
)
from loom_v2.slave.executor import execute_operation

ITEMS = [1, 3, 4, 3, 2, 10, 58]


def build_sort_closure() -> TaskClosure:
    return TaskClosure(
        closure_id="closure-sort-demo",
        data=DataApplication(
            logical_inputs=[
                ResourceRef(resource_id="input://items/1-3-4-3-2-10-58", version_or_digest="v1", identity_criterion="content_digest")
            ],
            identity_criterion="content_digest",
            expected_cardinality="7",
            terms=[TypedTerm(kind="loom.data.locality.v1", schema_ref="loom.data.locality/1", value={"locality": "workspace"})],
        ),
        program=ProgramApplication(operation_ref="loom://sort", success_semantics={"result": "sorted ascending"}),
        compute=ComputeSpec(operation_ref="loom://sort", typed_holes=[TypedHole(hole_id="h_sort")]),
        compute_application=ComputeApplication(result_expectation={"items": sorted(ITEMS)}),
        metadata={"goal": "sort the given list", "input_items": ITEMS},
    )


async def main() -> None:
    closure = build_sort_closure()
    print("closure_id:", closure.closure_id)
    print("operation_ref:", closure.program.operation_ref)
    print("canonical_digest:", closure.canonical_digest())
    print("result_expectation:", closure.compute_application.result_expectation)

    result = await execute_operation("sort", {"items": ITEMS})
    print("execution_digest:", result.digest)
    print("resource_ref:", result.resource_ref.resource_id)
    print("sorted_items:", result.value["items"])


if __name__ == "__main__":
    asyncio.run(main())
