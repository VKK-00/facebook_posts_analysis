from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RawSnapshotStore:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, category: str, stem: str, payload: Any) -> Path:
        category_dir = self.run_dir / category
        category_dir.mkdir(parents=True, exist_ok=True)
        target_path = category_dir / f"{stem}.json"
        target_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return target_path

    def write_manifest(self, payload: dict[str, Any]) -> Path:
        return self.write_json("", "manifest", payload)

