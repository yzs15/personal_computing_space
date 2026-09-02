SUMMARIZE = {"resource_id": "capability-package://summarize-bandgap/v1", "version_or_digest": "36cce99b33a7e5fb7c9a8b33cc1fba13f22476e78625a705e71ccff1cc2ed623"}
MERGE = {"resource_id": "capability-package://merge-bandgap/v1", "version_or_digest": "a4e52f66aa6b49fc8ff91ef0232de5946faef3eed02f4f0001f736c46cbab0c1"}

MAX_LIVE_NODES = 2


async def orchestrate(ctx: "OrchestrationContext", input_ref: "ResourceRef") -> "ResourceRef":
    document = await ctx.read_json(input_ref)
    partitions = document["partitions"]
    handles = []
    for start in range(0, len(partitions), MAX_LIVE_NODES):
        wave = partitions[start : start + MAX_LIVE_NODES]
        wave_handles = [ctx.emit_node(SUMMARIZE, [partition]) for partition in wave]
        for handle in wave_handles:
            await ctx.result(handle)
        handles.extend(wave_handles)
    summaries = [await ctx.result(handle) for handle in handles]
    merged = ctx.emit_node(MERGE, summaries)
    return await ctx.result(merged)
