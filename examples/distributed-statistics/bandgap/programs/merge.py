import json
import math
import sys


def main():
    document = json.load(sys.stdin)
    summaries = document["inputs"]

    count = 0
    total = 0.0
    sumsq = 0.0
    lo = None
    hi = None
    by_family = {}
    for summary in summaries:
        count += summary["count"]
        total += summary["sum"]
        sumsq += summary["count"] * (summary["stddev"] ** 2 + summary["mean"] ** 2)
        lo = summary["min"] if lo is None else min(lo, summary["min"])
        hi = summary["max"] if hi is None else max(hi, summary["max"])
        for family, stat in summary["by_family"].items():
            agg = by_family.setdefault(
                family,
                {"count": 0, "sum": 0.0, "sumsq": 0.0, "min": None, "max": None},
            )
            agg["count"] += stat["count"]
            agg["sum"] += stat["sum"]
            agg["sumsq"] += stat["count"] * (stat["stddev"] ** 2 + stat["mean"] ** 2)
            agg["min"] = stat["min"] if agg["min"] is None else min(agg["min"], stat["min"])
            agg["max"] = stat["max"] if agg["max"] is None else max(agg["max"], stat["max"])

    mean = total / count
    variance = max(0.0, sumsq / count - mean * mean)
    overall = {
        "count": count,
        "sum": round(total, 6),
        "min": round(lo, 6),
        "max": round(hi, 6),
        "mean": round(mean, 6),
        "stddev": round(math.sqrt(variance), 6),
    }
    by_family_report = {}
    for family, agg in sorted(by_family.items()):
        fam_mean = agg["sum"] / agg["count"]
        fam_var = max(0.0, agg["sumsq"] / agg["count"] - fam_mean * fam_mean)
        by_family_report[family] = {
            "count": agg["count"],
            "sum": round(agg["sum"], 6),
            "min": round(agg["min"], 6),
            "max": round(agg["max"], 6),
            "mean": round(fam_mean, 6),
            "stddev": round(math.sqrt(fam_var), 6),
        }
    print(json.dumps({"overall": overall, "by_family": by_family_report}))


if __name__ == "__main__":
    main()
