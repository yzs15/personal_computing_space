from loom_v2.contracts.refinement import is_monotonic_tightening


def test_numeric_budget_can_only_tighten():
    parent = {"op": "le", "field": "cpu_seconds", "value": 60}
    assert is_monotonic_tightening(parent, {"op": "le", "field": "cpu_seconds", "value": 30})
    assert not is_monotonic_tightening(parent, {"op": "le", "field": "cpu_seconds", "value": 90})
