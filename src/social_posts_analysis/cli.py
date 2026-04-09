from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from .analysis.service import AnalysisService
from .config import ProjectConfig, load_config
from .normalize import NormalizationService
from .paths import ProjectPaths
from .pipeline import CollectionService, PipelineRunner
from .reporting.service import ReportService, ReviewExportService

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _load_project(config_path: Path) -> tuple[Path, ProjectPaths, ProjectConfig]:
    root = config_path.resolve().parent.parent if config_path.parent.name == "config" else config_path.resolve().parent
    config = load_config(config_path)
    paths = ProjectPaths.from_config(root, config)
    paths.ensure()
    return root, paths, config


@app.command()
def collect(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = CollectionService(config=config, paths=paths)
    manifest = service.run(run_id=run_id)
    typer.echo(f"Collected run {manifest.run_id} with {len(manifest.posts)} posts.")


@app.command()
def normalize(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = NormalizationService(config=config, paths=paths)
    summary = service.run(run_id=run_id)
    typer.echo(f"Normalized run {summary['run_id']} into DuckDB/parquet tables.")


@app.command()
def analyze(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = AnalysisService(config=config, paths=paths)
    summary = service.run(run_id=run_id)
    typer.echo(f"Analyzed run {summary['run_id']} with providers {summary['providers']}.")


@app.command("review-export")
def review_export(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = ReviewExportService(config=config, paths=paths)
    outputs = service.run(run_id=run_id)
    typer.echo(f"Review files written: {', '.join(str(path) for path in outputs)}")


@app.command()
def report(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = ReportService(config=config, paths=paths)
    outputs = service.run(run_id=run_id)
    typer.echo(f"Report files written: {', '.join(str(path) for path in outputs)}")


@app.command("export-tables")
def export_tables(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = ReportService(config=config, paths=paths)
    outputs = service.run_tabular(run_id=run_id)
    typer.echo(f"Tabular exports written: {', '.join(str(path) for path in outputs)}")


@app.command("run-all")
def run_all(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    runner = PipelineRunner(config=config, paths=paths)
    summary = runner.run(run_id=run_id)
    typer.echo(
        "Completed run {run_id}. Report: {report}".format(
            run_id=summary["run_id"],
            report=summary["report_markdown"],
        )
    )


@app.command("run-many")
def run_many(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    passes: int = typer.Option(3, "--passes", min=1),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    original_passes = config.collector.multi_pass_runs
    config.collector.multi_pass_runs = passes
    try:
        runner = PipelineRunner(config=config, paths=paths)
        summary = runner.run(run_id=run_id)
    finally:
        config.collector.multi_pass_runs = original_passes
    typer.echo(
        "Completed multi-pass run {run_id}. Report: {report}".format(
            run_id=summary["run_id"],
            report=summary["report_markdown"],
        )
    )


if __name__ == "__main__":
    app()
