#!/usr/bin/env python3
"""Dünner Aufruf-Wrapper – eigentliche Logik in download_pmb_lists.py."""
import sys
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "download_pmb_lists",
    Path(__file__).parent / "download_pmb_lists.py",
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
sys.exit(mod.ensure_pmb_lists() and 0 or 0)
