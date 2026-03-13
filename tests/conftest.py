"""Shared fixtures for specimux-suite tests."""

import pytest
from pathlib import Path


@pytest.fixture
def tmp_output(tmp_path):
    """Provide a temporary output directory."""
    return tmp_path


@pytest.fixture
def test_data_dir():
    """Path to the test_data directory."""
    return Path(__file__).parent.parent / "test_data"


@pytest.fixture
def tiny_fastq(test_data_dir):
    """Path to the tiny synthetic FASTQ."""
    return test_data_dir / "tiny.fastq"
