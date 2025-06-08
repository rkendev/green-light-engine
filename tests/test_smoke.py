"""
Very light-weight tests – they run fast, do not hit the network,
and fail the build if core modules cannot be imported.

Run locally with:
    PREFECT_API_ENABLE=false  pytest -q
"""

import importlib
import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _disable_prefect_api(monkeypatch):
    """Ensure Prefect never starts its ephemeral API inside tests."""
    monkeypatch.setenv("PREFECT_API_ENABLE", "false")


def test_flow_imports():
    """The NYT Prefect flow can be imported and called without error."""
    flow_mod = importlib.import_module("flows.nytimes_flow")
    flow_mod.pull_latest_nyt.fn  # Prefect wrapper → .fn is the original
    assert callable(flow_mod.pull_latest_nyt)


def test_data_dir_exists():
    """`data/raw` hierarchy is present (created by repo)."""
    raw = Path("data/raw")
    assert raw.exists() and raw.is_dir()
