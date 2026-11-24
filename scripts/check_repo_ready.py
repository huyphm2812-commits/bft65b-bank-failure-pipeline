from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

MAX_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB
FORBIDDEN_EXTENSIONS = {".parquet", ".csv.gz", ".ipc", ".arrow"}
FORBIDDEN_PATTERNS = [
    re.compile(r"c:\\users\\", re.IGNORECASE),
    re.compile(r"c:/users/", re.IGNORECASE),
    re.compile(r"quanghuy", re.IGNORECASE),
]
REQUIRED_README_SECTIONS = [
    "Quick start",
    "Full run",
    "Outputs map",
    "Tests",
    "Sources and disclosure",
]


def check_repo(repo_root: Path) -> bool:
    print("==========================================================")
    print("  BFT65B REPOSITORY HYGIENE & PRE-COMMIT AUDIT")
    print(f"  Target Repository: {repo_root.resolve()}")
    print("==========================================================")

    all_passed = True

    # 1. File size & binary/data checks
    oversized: list[tuple[str, float]] = []
    forbidden_files: list[str] = []
    unexpected_csvs: list[str] = []

    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(repo_root).as_posix()
        # Skip git directory
        if rel.startswith(".git/"):
            continue

        size = path.stat().st_size
        if size > MAX_FILE_SIZE_BYTES:
            oversized.append((rel, size / 1024 / 1024))

        suffix = path.suffix.lower()
        if suffix in FORBIDDEN_EXTENSIONS:
            forbidden_files.append(rel)

        # CSV files are ONLY allowed in results_snapshot/ and must be < 1MB
        if suffix == ".csv":
            if not rel.startswith("results_snapshot/"):
                unexpected_csvs.append(rel)
            elif size > 1024 * 1024:
                oversized.append((rel, size / 1024 / 1024))

    if oversized:
        all_passed = False
        print(f"[FAIL] Files exceeding 5 MB limit ({len(oversized)}):")
        for f, s in oversized:
            print(f"       - {f} ({s:.2f} MB)")
    else:
        print("[PASS] No files exceed 5 MB limit.")

    if forbidden_files:
        all_passed = False
        print(f"[FAIL] Forbidden data/binary formats committed ({len(forbidden_files)}):")
        for f in forbidden_files:
            print(f"       - {f}")
    else:
        print("[PASS] No raw/parquet/arrow data files found in repository.")

    if unexpected_csvs:
        all_passed = False
        print(f"[FAIL] CSV files outside results_snapshot/ ({len(unexpected_csvs)}):")
        for f in unexpected_csvs:
            print(f"       - {f}")
    else:
        print("[PASS] No data CSV files committed outside results_snapshot/.")

    # 2. Hardcoded paths / usernames in code and configs
    hardcoded_leaks: list[tuple[str, str]] = []
    scan_exts = {".py", ".yaml", ".yml", ".md", ".json", ".ini", ".ps1", ".sh"}

    for path in repo_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in scan_exts:
            continue
        rel = path.relative_to(repo_root).as_posix()
        if rel.startswith(".git/") or rel.startswith("results_snapshot/") or rel == "scripts/check_repo_ready.py":
            continue

        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue

        for pat in FORBIDDEN_PATTERNS:
            match = pat.search(content)
            if match:
                # Find matching line
                for line in content.splitlines():
                    if pat.search(line):
                        hardcoded_leaks.append((rel, line.strip()[:80]))
                        break

    if hardcoded_leaks:
        all_passed = False
        print(f"[FAIL] Absolute paths / user names leaked in code ({len(hardcoded_leaks)}):")
        for f, line in hardcoded_leaks[:10]:
            print(f"       - {f}: {line}")
    else:
        print("[PASS] Zero hardcoded absolute paths or usernames found.")

    # 3. Pinned requirements check
    req_file = repo_root / "requirements.txt"
    if req_file.exists():
        lines = [l.strip() for l in req_file.read_text(encoding="utf-8").splitlines() if l.strip() and not l.startswith("#")]
        unpinned = [l for l in lines if "==" not in l]
        if unpinned:
            all_passed = False
            print(f"[FAIL] Unpinned packages in requirements.txt: {unpinned}")
        else:
            print(f"[PASS] All {len(lines)} requirements pinned with exact versions.")
    else:
        all_passed = False
        print("[FAIL] requirements.txt missing.")

    # 4. README required sections
    readme_file = repo_root / "README.md"
    if readme_file.exists():
        readme_text = readme_file.read_text(encoding="utf-8")
        missing_sections = [sec for sec in REQUIRED_README_SECTIONS if sec.lower() not in readme_text.lower()]
        if missing_sections:
            all_passed = False
            print(f"[FAIL] README missing required sections: {missing_sections}")
        else:
            print("[PASS] README contains all required sections.")
    else:
        all_passed = False
        print("[FAIL] README.md missing.")

    # 5. Git Status / Ignored folders staged
    git_dir = repo_root / ".git"
    if git_dir.exists():
        try:
            status = subprocess.check_output(["git", "status", "--porcelain"], cwd=repo_root, text=True)
            forbidden_staged = []
            for line in status.splitlines():
                entry = line[3:].strip()
                if any(entry.startswith(prefix) for prefix in ["02_local_workspace", "data", "outputs", "logs", "manifests", "tmp"]):
                    forbidden_staged.append(entry)
            if forbidden_staged:
                all_passed = False
                print(f"[FAIL] Workspace/data directories staged in Git: {forbidden_staged}")
            else:
                print("[PASS] Git status clean (no ignored workspace folders staged).")
        except Exception as e:
            print(f"[WARN] Could not inspect git status: {e}")
    else:
        print("[INFO] Git not yet initialized in 01_github_repo (will be initialized for upload).")

    # 6. Run Pytest Suite
    print("\nRunning automated unit and integration tests (pytest -q)...")
    try:
        res = subprocess.run(["pytest", "-q"], cwd=repo_root, capture_output=True, text=True)
        if res.returncode == 0:
            print(f"[PASS] Test suite passed:\n       {res.stdout.strip()}")
        else:
            all_passed = False
            print(f"[FAIL] Test suite failed (code {res.returncode}):\n{res.stdout}\n{res.stderr}")
    except Exception as exc:
        all_passed = False
        print(f"[FAIL] Failed to run pytest: {exc}")

    print("\n==========================================================")
    status_str = "READY FOR GITHUB UPLOAD" if all_passed else "REMEDIAL ACTIONS REQUIRED"
    print(f"  FINAL VERDICT: {status_str}")
    print("==========================================================")
    return all_passed


if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parent.parent
    ok = check_repo(repo_root)
    sys.exit(0 if ok else 1)
