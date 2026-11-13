"""Shared runner for scenario modules: runs the logic, prints PASS/FAIL with measured values, saves JSON."""
import json
import os
import pathlib
import time


def report(name, result):
    status = "PASS" if result.get("pass") else ("ERROR" if result.get("error") else "FAIL")
    print(f"\n=== {name}: {status} ===")
    for k, v in (result.get("checks") or {}).items():
        print(f"  [{'PASS' if v else 'FAIL'}] {k}")
    print(json.dumps({k: v for k, v in result.items() if k != "checks"}, indent=1, default=str)[:6000])
    out = pathlib.Path(os.environ.get("RESULTS_DIR", "/srv/results"))
    try:
        out.mkdir(parents=True, exist_ok=True)
        f = out / f"{name}-{time.strftime('%Y%m%d-%H%M%S')}.json"
        f.write_text(json.dumps(dict(result, status=status), indent=1, default=str))
        print(f"saved {f}")
    except OSError:
        pass
    return 0 if status == "PASS" else 1


def run_in_app_scenario(name):
    from app import chaos
    rid = chaos.start_run(name)
    res = chaos.execute(name, rid)
    return report(name, res)
