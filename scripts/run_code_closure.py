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
from loom_v2.slave.executor import ProcessJSONStdioV1Adapter


def build_run_code_closure() -> TaskClosure:
    return TaskClosure(
        closure_id="closure-run-code-demo",
        data=DataApplication(
            logical_inputs=[
                ResourceRef(resource_id="input://value/3", version_or_digest="v1", identity_criterion="content_digest")
            ],
            identity_criterion="content_digest",
            expected_cardinality="1",
            terms=[TypedTerm(kind="loom.data.locality.v1", schema_ref="loom.data.locality/1", value={"locality": "workspace"})],
        ),
        program=ProgramApplication(operation_ref="loom://test_double"),
        compute=ComputeSpec(operation_ref="loom://test_double", typed_holes=[TypedHole(hole_id="h_run_code")]),
        compute_application=ComputeApplication(result_expectation={"value": 6}),
        metadata={"goal": "run the supplied value through a capability package", "input_value": 3},
    )


async def main() -> None:
    closure = build_run_code_closure()
    print("closure_id:", closure.closure_id)
    print("operation_ref:", closure.program.operation_ref)
    print("canonical_digest:", closure.canonical_digest())
    print("result_expectation:", closure.compute_application.result_expectation)

    program = b'import json,sys; value=json.load(sys.stdin)["value"]; print(json.dumps({"value": value * 2}))'
    result = await ProcessJSONStdioV1Adapter().invoke({"value": 3}, program=program)
    print("execution_digest:", result.digest)
    print("resource_ref:", result.resource_ref.resource_id)
    print("result:", result.value)


if __name__ == "__main__":
    asyncio.run(main())
