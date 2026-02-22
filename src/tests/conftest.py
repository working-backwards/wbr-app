# SPDX-License-Identifier: Apache-2.0
"""
Shared fixtures for the WBR test suite.

Provides scenario loading utilities that create WBR objects and deck outputs
from the golden-output test cases in src/unit_test_case/scenario_*.
"""
import os
from functools import lru_cache
from pathlib import Path

import yaml

import src.wbr as wbr
from src import validator
from src.controller_utility import SafeLineLoader, get_wbr_deck
from src.data_loader import DataLoader

SCENARIO_DIR = Path(os.path.dirname(__file__)).parent / "unit_test_case"


@lru_cache(maxsize=None)
def _load_scenario(scenario_name):
    """Load a scenario's WBR object and deck, cached per scenario name.

    Multiple test cases within the same scenario share one WBR + deck instance,
    so we cache to avoid redundant computation.
    """
    scenario_path = SCENARIO_DIR / scenario_name
    csv_file = str(scenario_path / "original.csv")
    config_file_path = str(scenario_path / "config.yaml")

    config = yaml.load(open(config_file_path), SafeLineLoader)
    data_loader = DataLoader(cfg=config, csv_data=csv_file)
    wbr_validator = validator.WBRValidator(cfg=config, daily_df=data_loader.daily_df)
    wbr_validator.validate_yaml()
    wbr1 = wbr.WBR(config, daily_df=wbr_validator.daily_df)
    deck = get_wbr_deck(wbr1)
    return wbr1, deck


def load_scenario(scenario_name):
    """Public API: returns (wbr1, deck) for a given scenario directory name."""
    return _load_scenario(scenario_name)


def collect_all_test_cases():
    """Walk all scenario directories and yield (scenario_name, test_dict) tuples.

    Each tuple represents one golden-output test case that should be validated.
    Used by test_wbr_scenarios.py to parametrize across all 42 test cases.
    """
    cases = []
    for entry in sorted(SCENARIO_DIR.iterdir()):
        if not entry.is_dir() or "scenario" not in entry.name:
            continue
        test_config_file = entry / "testconfig.yml"
        test_config = yaml.safe_load(open(test_config_file))
        for test_entry in test_config["tests"]:
            test_dict = test_entry["test"]
            test_id = f"{entry.name}/tc{test_dict['test_case_no']}_{test_dict['metric_name']}"
            cases.append((entry.name, test_dict, test_id))
    return cases
