import pytest
import os
import subprocess
import sys

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

@pytest.fixture(scope="session", autouse=True)
def generate_fixtures():
    os.makedirs(FIXTURES_DIR, exist_ok=True)
    # Only regenerate if fixtures are missing
    expected = 18
    existing = len([f for f in os.listdir(FIXTURES_DIR) if f.endswith((".xlsx", ".csv", ".tsv"))])
    if existing < expected:
        subprocess.run(
            [sys.executable, os.path.join(os.path.dirname(__file__), "generate_fixtures.py")],
            check=True,
        )

@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR

@pytest.fixture
def fixture_path(fixtures_dir):
    def _get(name: str) -> str:
        path = os.path.join(fixtures_dir, name)
        assert os.path.exists(path), f"Fixture not found: {path}"
        return path
    return _get
