from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from .analysis.service import AnalysisService
from .collectors.instagram_web import InstagramWebCollector
from .config import ProjectConfig, load_config
from .history import HistoricalBackfillService, HistoryAnalysisService, HistoryReportService
from .normalize import NormalizationService
from .openclaw import OpenClawExportService
from .paths import ProjectPaths, project_root_for_config, relative_output_paths_warning
from .pipeline import CollectionService, PipelineRunner
from .reporting.service import ReportService, ReviewExportService
from .utils import make_run_id

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _load_project(config_path: Path) -> tuple[Path, ProjectPaths, ProjectConfig]:
    root = project_root_for_config(config_path)
    config = load_config(config_path)
    warning = relative_output_paths_warning(config_path, config)
    if warning:
        typer.echo(f"Warning: {warning}", err=True)
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


@app.command("doctor-instagram-web")
def doctor_instagram_web(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    target_url: Optional[str] = typer.Option(None, "--target-url"),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    resolved_run_id = run_id or make_run_id()
    collector = InstagramWebCollector(config)
    diagnostic = collector.diagnose_browser_session(target_url)
    diagnostic_dir = paths.raw_root / "_diagnostics" / resolved_run_id
    diagnostic_dir.mkdir(parents=True, exist_ok=True)
    diagnostic_path = diagnostic_dir / "instagram_web_session.json"
    diagnostic_path.write_text(
        json.dumps(diagnostic, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    typer.echo(f"Instagram web diagnostic written: {diagnostic_path}")
    typer.echo(f"Instagram web diagnostic status: {diagnostic['status']}")
    for warning in diagnostic.get("warnings") or []:
        typer.echo(f"Warning: {warning}", err=True)


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


@app.command("openclaw-export")
def openclaw_export(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    run_id: Optional[str] = typer.Option(None, "--run-id"),
    history_run_id: Optional[str] = typer.Option(None, "--history-run-id"),
) -> None:
    if run_id and history_run_id:
        typer.echo("--run-id and --history-run-id are mutually exclusive.", err=True)
        raise typer.Exit(1)
    _, paths, config = _load_project(config_path)
    service = OpenClawExportService(config=config, paths=paths)
    try:
        outputs = service.run_history(history_run_id=history_run_id) if history_run_id else service.run(run_id=run_id)
    except RuntimeError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"OpenClaw bundle written: {outputs.bundle_path}")
    typer.echo(f"OpenClaw brief written: {outputs.brief_path}")


@app.command("history-run")
def history_run(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    history_run_id: Optional[str] = typer.Option(None, "--history-run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    service = HistoricalBackfillService(config=config, paths=paths)
    summary = service.run(history_run_id=history_run_id)
    typer.echo(f"Historical run completed: {summary['history_run_id']}")
    typer.echo(f"Historical run status: {summary['status']}")


@app.command("history-report")
def history_report(
    config_path: Path = typer.Option(Path("config/project.yaml"), "--config", exists=True, readable=True),
    history_run_id: str = typer.Option(..., "--history-run-id"),
) -> None:
    _, paths, config = _load_project(config_path)
    HistoryAnalysisService(config=config, paths=paths).run(history_run_id=history_run_id)
    service = HistoryReportService(config=config, paths=paths)
    outputs = service.run(history_run_id=history_run_id)
    typer.echo(f"History report files written: {', '.join(str(path) for path in outputs)}")


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
