"""Shared test configuration and fixtures."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "e2e: end-to-end tests requiring live SPCS cluster (deselect with '-m \"not e2e\"')",
    )
    config.addinivalue_line(
        "markers",
        "local: local integration tests using docker compose (deselect with '-m \"not local\"')",
    )
