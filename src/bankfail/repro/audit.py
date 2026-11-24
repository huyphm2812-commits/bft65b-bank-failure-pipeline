"""Part 8: reproducibility, repository hygiene and run manifests."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path
from typing import Any

REQUIRED_FILES_PACKAGE = [
    ".gitignore",
    "pytest.ini",
    "requirements.txt",
    "README.md",
    ".python-version",
    "config/config.yaml",
    "config/config_smoke.yaml",
    "scripts/run_pipeline.py",
    "scripts/run_smoke.ps1",
    "scripts/run_smoke.sh",
    "scripts/run_full.ps1",
    "scripts/run_full.sh",
    "scripts/export_results.py",
    "scripts/check_repo_ready.py",
    "tests/test_pipeline.py",
    "tests/fixtures/fake_fdic_server.py",
    "src/bankfail/__init__.py",
    "src/bankfail/acquisition/__init__.py",
    "src/bankfail/quality/__init__.py",
    "src/bankfail/panel/__init__.py",
    "src/bankfail/benchmarks/__init__.py",
    "src/bankfail/processing/__init__.py",
    "src/bankfail/spark/__init__.py",
    "src/bankfail/modeling/__init__.py",
    "src/bankfail/repro/__init__.py",
    "docs/DECISIONS_LOG.md",
    "docs/EVIDENCE_MAP.md",
    "docs/CITATIONS.md",
    "docs/AI_USAGE.md",
    "docs/CHANGES.md",
]

ALLOWED_DIRS_PACKAGE = {
    "config", "src", "scripts", "tests", "docs", "results_snapshot",
    "__pycache__", ".pytest_cache", ".venv", ".git"
}

REQUIRED_FILES_FLAT = [
    ".gitignore",
    "pytest.ini",
    "requirements.txt",
    "README.md",
    "config_config.yaml",
    "run_pipeline.py",
    "test_pipeline.py",
    "p1_fdic_client.py",
    "p1_storage.py",
    "p1_downloader.py",
    "p1_acquisition_pipeline.py",
    "p1_validation.py",
    "p2_quality_audit.py",
    "p3_panel.py",
    "p4_benchmark.py",
    "p5_processing.py",
    "p6_spark.py",
    "p7_modeling.py",
    "p8_reproducibility.py",
]
IGNORED_GENERATED_DIRS = {"__pycache__", ".pytest_cache", ".venv", "data", "manifests", "logs", "outputs", ".bft65b_spark_tmp"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _requirements_status(path: Path) -> dict[str, Any]:
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip() and not line.lstrip().startswith("#")]
    unpinned = [line for line in lines if "==" not in line]
    return {"count": len(lines), "unpinned": unpinned, "all_pinned": not unpinned}


def audit_repository(repo_dir: Path) -> dict[str, Any]:
    """Check the deliverable structure without requiring downloaded data."""
    is_package_layout = (repo_dir / "src" / "bankfail").exists()
    
    if is_package_layout:
        required = REQUIRED_FILES_PACKAGE
        missing = [name for name in required if not (repo_dir / name).exists()]
        nested_dirs = sorted(p.name for p in repo_dir.iterdir() if p.is_dir() and p.name not in ALLOWED_DIRS_PACKAGE)
        generated_dirs = sorted(p.name for p in repo_dir.iterdir() if p.is_dir() and p.name in IGNORED_GENERATED_DIRS)
        config_path = repo_dir / "config" / "config.yaml"
    else:
        required = REQUIRED_FILES_FLAT
        missing = [name for name in required if not (repo_dir / name).is_file()]
        nested_dirs = sorted(path.name for path in repo_dir.iterdir() if path.is_dir() and path.name not in IGNORED_GENERATED_DIRS)
        generated_dirs = sorted(path.name for path in repo_dir.iterdir() if path.is_dir() and path.name in IGNORED_GENERATED_DIRS)
        config_path = repo_dir / "config_config.yaml"

    requirements = _requirements_status(repo_dir / "requirements.txt") if (repo_dir / "requirements.txt").exists() else {"all_pinned": False, "unpinned": ["requirements.txt missing"]}
    readme = (repo_dir / "README.md").read_text(encoding="utf-8") if (repo_dir / "README.md").exists() else ""
    config = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    pytest_ini = (repo_dir / "pytest.ini").read_text(encoding="utf-8") if (repo_dir / "pytest.ini").exists() else ""
    
    checks = {
        "required_files_present": not missing,
        "no_unexpected_nested_directories": not nested_dirs,
        "requirements_all_pinned": requirements["all_pinned"],
        "readme_has_sources": "Sources" in readme and "fdic.gov" in readme,
        "readme_discloses_ai": "AI" in readme or "disclosure" in readme.lower(),
        "config_has_seed": "random_seed" in config,
        "pytest_configured": "testpaths" in pytest_ini or "pythonpath" in pytest_ini,
    }
    return {
        "ok": all(checks.values()),
        "layout": "package" if is_package_layout else "flat",
        "checks": checks,
        "missing_required_files": missing,
        "unexpected_nested_directories": nested_dirs,
        "ignored_generated_directories": generated_dirs,
        "requirements": requirements,
    }


def _package_versions(requirements_path: Path) -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        package = line.split("==", 1)[0].strip()
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def write_run_manifest(repo_dir: Path, output_dir: Path, config_path: Path, command: list[str], extra: dict[str, Any] | None = None) -> Path:
    """Write an auditable run manifest outside the source repository."""
    output_dir.mkdir(parents=True, exist_ok=True)
    source_hashes = {}
    
    # Hash files in repository
    scan_paths = [repo_dir]
    if (repo_dir / "src").exists():
        scan_paths.extend([repo_dir / "src", repo_dir / "scripts", repo_dir / "tests", repo_dir / "config"])
        
    for sp in scan_paths:
        for path in sorted(sp.rglob("*")):
            if path.is_file() and path.suffix.lower() in {".py", ".yaml", ".md", ".txt", ".ini"}:
                rel = path.relative_to(repo_dir).as_posix()
                source_hashes[rel] = sha256_file(path)

    manifest = {
        "manifest_version": "part8-v2",
        "python": sys.version,
        "platform": platform.platform(),
        "command": command,
        "config_path": str(config_path.resolve()),
        "config_sha256": sha256_file(config_path) if config_path.exists() else None,
        "installed_packages": _package_versions(repo_dir / "requirements.txt") if (repo_dir / "requirements.txt").exists() else {},
        "source_files": source_hashes,
        "extra": extra or {},
    }
    out_file = output_dir / "p8_reproducibility_manifest.json"
    out_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return out_file
