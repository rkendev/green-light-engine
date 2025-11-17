"""
Core library package for the Green Light Engine project.

This package exposes reusable functionality for data ingestion,
preparation, feature engineering, and modeling.

High level modules

gle.ingest_nyt      New York Times books list ingestion
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("green-light-engine")
except PackageNotFoundError:
    __version__ = "0.0.0"
