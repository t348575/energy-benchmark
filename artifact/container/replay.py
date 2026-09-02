#!/usr/bin/env python3
"""Non-destructive IISWC 2026 data-analysis replay."""

from __future__ import annotations

import argparse
import ast
import csv
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


PAPER_PLOTS_DIR = Path("/opt/paper-plots")
RUN_DISCOVERY_SCRIPTS = ("preprocess-data.py", "fig5cd_preprocess.py")
ENERGY_BENCH = Path("/usr/local/bin/nvme-energy-bench")
ENERGY_BENCH_PLOTS = Path("/opt/nvme-energy-bench/plots")
RUN_NAME = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9_.-]*-20\d{2}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"
)


def paper_run_directories(paper_plots_dir: Path = PAPER_PLOTS_DIR) -> list[str]:
    runs: set[str] = set()
    for name in RUN_DISCOVERY_SCRIPTS:
        script = paper_plots_dir / name
        tree = ast.parse(script.read_text(encoding="utf-8"), filename=str(script))
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                runs.update(RUN_NAME.findall(node.value))
    if not runs:
        raise ValueError(
            f"no experiment directories found in {', '.join(RUN_DISCOVERY_SCRIPTS)}"
        )
    return sorted(runs)


def figure_scripts(paper_plots_dir: Path = PAPER_PLOTS_DIR) -> list[str]:
    workflow = paper_plots_dir / "fig_workflow.py"
    tree = ast.parse(workflow.read_text(encoding="utf-8"), filename=str(workflow))
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "EXPERIMENTS"
            and isinstance(node.value, ast.List)
        ):
            return [
                f"{element.value}.py"
                for element in node.value.elts
                if isinstance(element, ast.Constant) and isinstance(element.value, str)
            ]
    raise ValueError(f"no EXPERIMENTS list found in {workflow}")


def run_checked(
    command: list[str],
    cwd: Path,
    log_path: Path,
    label: str,
    environment: dict[str, str] | None = None,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        result = subprocess.run(
            command,
            cwd=cwd,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            env=environment,
        )
    if result.returncode:
        tail = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-40:]
        raise RuntimeError(
            f"{label} failed with exit code {result.returncode}; see {log_path}\n"
            + "\n".join(tail)
        )


def find_results_root(dataset: Path, requested: str | None, runs: list[str]) -> Path:
    if requested:
        relative = Path(requested)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("--results-subdir must be relative to the dataset root")
        candidates = [dataset / relative]
    else:
        candidates = [
            dataset / "results",
            dataset / "data" / "results",
            dataset / "data" / "processed" / "results",
            dataset,
        ]

    for candidate in candidates:
        if candidate.is_dir() and all((candidate / run).is_dir() for run in runs):
            return candidate.resolve()

    existing = next((path for path in candidates if path.is_dir()), None)
    if existing is not None:
        missing = [run for run in runs if not (existing / run).is_dir()]
        preview = ", ".join(missing[:10])
        remainder = f" (and {len(missing) - 10} more)" if len(missing) > 10 else ""
        raise ValueError(
            f"{existing} is missing {len(missing)} paper-plot experiment directories: "
            f"{preview}{remainder}"
        )
    raise ValueError(
        "could not find the results tree; checked "
        + ", ".join(str(path) for path in candidates)
    )


def validate_run(source: Path) -> None:
    missing = [name for name in ("config.yaml", "info.json", "data") if not (source / name).exists()]
    if missing:
        raise ValueError(f"{source.name}: missing required input(s): {', '.join(missing)}")
    if not (source / "data").is_dir():
        raise ValueError(f"{source.name}: data is not a directory")


def prepare_run(source: Path, destination: Path, reuse_plots: bool) -> None:
    """Create a writable run shell whose immutable inputs are symlinks."""
    validate_run(source)
    destination.mkdir(parents=True, exist_ok=False)
    for name in ("config.yaml", "info.json", "data"):
        (destination / name).symlink_to(source / name, target_is_directory=name == "data")
    if reuse_plots:
        plots = source / "plots"
        if not plots.is_dir():
            raise ValueError(f"{source.name}: --skip-nvme-energy-bench requires plots/")
        (destination / "plots").symlink_to(plots, target_is_directory=True)


def prepare_paper_output_directories(figures: Path) -> None:
    figures.mkdir(parents=True, exist_ok=True)
    for target in ("cpu", "ssd", "both", "system"):
        (figures / "fig" / target / "engine").mkdir(parents=True, exist_ok=True)


def clean_known_outputs(output: Path) -> None:
    for relative in ("work", "figures", "nvme-energy-bench-plots", "logs"):
        candidate = output / relative
        if candidate.is_symlink() or candidate.is_file():
            candidate.unlink()
        elif candidate.is_dir():
            shutil.rmtree(candidate)
    for relative in ("run.json", "run-manifest.txt", "plot-status.csv"):
        candidate = output / relative
        if candidate.exists() or candidate.is_symlink():
            candidate.unlink()


def reproduce(
    dataset: Path,
    output: Path,
    results_subdir: str | None,
    skip_nvme_plots: bool,
    skip_nvme_energy_bench: bool,
) -> int:
    dataset = dataset.resolve(strict=True)
    output = output.resolve()
    if not dataset.is_dir():
        raise ValueError(f"dataset directory does not exist: {dataset}")
    if output == dataset or dataset in output.parents or output in dataset.parents:
        raise ValueError("dataset and output directories must not overlap")

    runs = paper_run_directories()
    results = find_results_root(dataset, results_subdir, runs)
    output.mkdir(parents=True, exist_ok=True)
    clean_known_outputs(output)

    workspace = output / "work"
    work_results = workspace / "results"
    tool_work = workspace / "tool"
    figures = output / "figures"
    generated = output / "nvme-energy-bench-plots"
    logs = output / "logs"
    work_results.mkdir(parents=True)
    tool_work.mkdir(parents=True)
    generated.mkdir(parents=True)
    (workspace / "iiswcdata").mkdir(parents=True)
    prepare_paper_output_directories(figures)
    shutil.copytree(ENERGY_BENCH_PLOTS, tool_work / "plots")
    (workspace / "plots").symlink_to(figures, target_is_directory=True)

    (output / "run-manifest.txt").write_text("\n".join(runs) + "\n", encoding="utf-8")
    statuses: list[dict[str, str]] = []
    nvme_environment = os.environ.copy()
    if skip_nvme_plots:
        nvme_environment["SKIP_PLOT"] = "1"

    for index, run in enumerate(runs, start=1):
        source = results / run
        destination = work_results / run
        prepare_run(source, destination, reuse_plots=skip_nvme_energy_bench)
        if skip_nvme_energy_bench:
            print(f"[{index:02d}/{len(runs):02d}] reusing plots: {run}", flush=True)
            statuses.append({"experiment": run, "status": "REUSED"})
        else:
            print(f"[{index:02d}/{len(runs):02d}] nvme-energy-bench plot: {run}", flush=True)
            run_checked(
                [str(ENERGY_BENCH), "plot", "--folder", str(destination)],
                tool_work,
                logs / "nvme-energy-bench" / f"{run}.log",
                f"nvme-energy-bench plot for {run}",
                nvme_environment,
            )
            statuses.append({"experiment": run, "status": "PASS"})

    paper_plot_scripts = [
        "preprocess-data.py",
        "fig5cd_preprocess.py",
        *figure_scripts(),
        "fig_sweep.py",
    ]
    for script in paper_plot_scripts:
        print(f"Generating paper figures: {script}", flush=True)
        command = [sys.executable, str(PAPER_PLOTS_DIR / script)]
        if script in ("preprocess-data.py", "fig5cd_preprocess.py"):
            command += ["--in-dir", "results"]
        run_checked(
            command,
            workspace,
            logs / "paper-fig" / f"{script}.log",
            f"paper figure generation ({script})",
        )

    if not skip_nvme_energy_bench:
        for run in runs:
            source_plots = work_results / run / "plots"
            if not source_plots.is_dir():
                raise RuntimeError(f"nvme-energy-bench produced no plots directory for {run}")
            shutil.move(str(source_plots), str(generated / run))

    with (output / "plot-status.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=("experiment", "status"))
        writer.writeheader()
        writer.writerows(statuses)

    paper_pdfs = sorted(figures.rglob("*.pdf"))
    if not paper_pdfs:
        raise RuntimeError("paper-plot generation completed but produced no PDF figures")

    metadata = {
        "completed_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": str(dataset),
        "results_root": str(results),
        "experiment_count": len(runs),
        "paper_pdf_count": len(paper_pdfs),
        "container_platform": platform.platform(),
        "machine": platform.machine(),
        "nvme_energy_bench_binary": str(ENERGY_BENCH),
        "nvme_energy_bench_ran": not skip_nvme_energy_bench,
        "nvme_energy_bench_python_plots_skipped": skip_nvme_plots,
    }
    (output / "run.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    shutil.rmtree(workspace)

    print(f"Parsed experiment directories: {len(runs)}")
    print(f"Generated paper PDFs: {len(paper_pdfs)}")
    print(f"Replay output: {output}")
    print("IISWC 2026 artifact replay: PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=Path("/dataset"))
    parser.add_argument("--output", type=Path, default=Path("/output"))
    parser.add_argument("--results-subdir")
    parser.add_argument(
        "--skip-nvme-plots",
        action="store_true",
        help="skip nvme-energy-bench's bundled Python rendering after it writes plot-data JSON/spec files",
    )
    parser.add_argument(
        "--skip-nvme-energy-bench",
        action="store_true",
        help="reuse each run's existing dataset plots/ directory instead of running nvme-energy-bench",
    )
    args = parser.parse_args()
    try:
        return reproduce(
            args.dataset,
            args.output,
            args.results_subdir,
            args.skip_nvme_plots,
            args.skip_nvme_energy_bench,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
