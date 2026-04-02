from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from facebook_posts_analysis.config import load_config
from facebook_posts_analysis.paths import ProjectPaths


@pytest.fixture()
def project_root(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    root.mkdir()
    shutil.copytree(Path("config"), root / "config")
    shutil.copytree(Path("tests/fixtures/raw_run"), root / "data/raw/20260402T120000Z")
    return root


@pytest.fixture()
def project_config(project_root: Path):
    config_path = project_root / "config/project.yaml"
    return load_config(config_path)


@pytest.fixture()
def project_paths(project_root: Path, project_config):
    paths = ProjectPaths.from_config(project_root, project_config)
    paths.ensure()
    return paths
