import json
import math
import sys


def main():
    document = json.load(sys.stdin)
    summaries = document["inputs"]
    count = sum(item["count"] for item in summaries)
    total = sum(item["sum"] for item in summaries)
    sumsq = sum(item["sumsq"] for item in summaries)
    lo = min(item["min"] for item in summaries)
    hi = max(item["max"] for item in summaries)
    mean = total / count
    variance = sumsq / count - mean * mean
    print(json.dumps({
        "count": count,
        "sum": total,
        "min": lo,
        "max": hi,
        "sumsq": sumsq,
        "mean": mean,
        "stddev": math.sqrt(max(0.0, variance)),
    }))


if __name__ == "__main__":
    main()
