"""Chaos scenario 'poison' (same logic as the in-app button). Run: docker compose run --rm tools python -m sim.poison"""
import sys

from sim.common import run_in_app_scenario

if __name__ == "__main__":
    sys.exit(run_in_app_scenario("poison"))
