# -*- coding: utf-8 -*-
"""
Observatory Experiment Utilities
================================

This package hosts the "paper-grade" experiment pipeline building blocks:
- episode dataset validation/expansion
- headless batch runner (future)
- tick metrics extraction (future)

Keeping these utilities under `observatory/` (instead of `tools/`) allows
the web UI to call them without depending on script-only modules.
"""

from . import io, storage
from .dataset import DatasetValidationError, estimate_total_ticks, expand_dataset, validate_and_normalize_dataset
from .runner import RunOptions, export_expanded_ticks, load_dataset_ticks, make_run_id, run_dataset
from .storage import DatasetFileRef, list_dataset_files, list_runs

__all__ = [
    "io",
    "storage",
    "DatasetFileRef",
    "DatasetValidationError",
    "RunOptions",
    "estimate_total_ticks",
    "expand_dataset",
    "export_expanded_ticks",
    "list_dataset_files",
    "list_runs",
    "load_dataset_ticks",
    "make_run_id",
    "run_dataset",
    "validate_and_normalize_dataset",
]
