from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import ProjectConfig


@dataclass(slots=True)
class ProjectPaths:
    root: Path
    raw_root: Path
    processed_root: Path
    review_root: Path
    reports_root: Path
    database_path: Path

    @classmethod
    def from_config(cls, root: Path, config: ProjectConfig) -> "ProjectPaths":
        return cls(
            root=root,
            raw_root=root / config.paths.raw_dir,
            processed_root=root / config.paths.processed_dir,
            review_root=root / config.paths.review_dir,
            reports_root=root / config.paths.reports_dir,
            database_path=root / config.paths.database_path,
        )

    def ensure(self) -> None:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.processed_root.mkdir(parents=True, exist_ok=True)
        self.review_root.mkdir(parents=True, exist_ok=True)
        self.reports_root.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def run_raw_dir(self, run_id: str) -> Path:
        return self.raw_root / run_id

    def latest_run_id(self) -> str | None:
        run_ids = self.list_run_ids()
        return run_ids[-1] if run_ids else None

    def list_run_ids(self) -> list[str]:
        if not self.raw_root.exists():
            return []
        run_dirs = [item for item in self.raw_root.iterdir() if item.is_dir() and (item / "manifest.json").exists()]
        return sorted(item.name for item in run_dirs)
