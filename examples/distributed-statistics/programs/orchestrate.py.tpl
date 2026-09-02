SUMMARIZE = SUMMARIZE_REF_PLACEHOLDER
MERGE = MERGE_REF_PLACEHOLDER

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
