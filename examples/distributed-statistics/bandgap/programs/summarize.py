import json
import math
import sys


def stats(values):
    n = len(values)
    total = sum(values)
    sumsq = sum(v * v for v in values)
    mean = total / n
    variance = max(0.0, sumsq / n - mean * mean)
    return {
        "count": n,
        "sum": round(total, 6),
        "min": round(min(values), 6),
        "max": round(max(values), 6),
        "mean": round(mean, 6),
        "stddev": round(math.sqrt(variance), 6),
    }


def main():
    document = json.load(sys.stdin)
    items = document["items"]
    values = [item["e_gap"] for item in items]
    by_family = {}
    for item in items:
        by_family.setdefault(item["family"], []).append(item["e_gap"])
    report = {"by_family": {family: stats(vals) for family, vals in sorted(by_family.items())}}
    report.update(stats(values))
    print(json.dumps(report))


if __name__ == "__main__":
    main()
