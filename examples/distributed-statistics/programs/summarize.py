import json
import sys


def main():
    document = json.load(sys.stdin)
    items = document["items"]
    print(json.dumps({
        "count": len(items),
        "sum": sum(items),
        "min": min(items),
        "max": max(items),
        "sumsq": sum(x * x for x in items),
    }))


if __name__ == "__main__":
    main()
