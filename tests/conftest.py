"""A temporary git repo built from tests/fixtures/base, and a way to run the checker on a change."""

import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "check_provider.py"
BASE_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "base"


@dataclass
class Outcome:
    code: int
    stdout: str
    stderr: str


class Repo:
    def __init__(self, root: Path) -> None:
        self.root = root

    def git(self, *args: str) -> None:
        subprocess.run(
            [
                "git",
                "-C",
                str(self.root),
                "-c",
                "user.name=t",
                "-c",
                "user.email=t@example.test",
                *args,
            ],
            check=True,
            capture_output=True,
        )

    def write(self, relative: str, text: str) -> None:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def edit(self, relative: str, old: str, new: str) -> None:
        text = (self.root / relative).read_text()
        assert old in text
        self.write(relative, text.replace(old, new))

    def check(self, change: Callable[["Repo"], None]) -> Outcome:
        """Commit `change` on a branch off main and run the checker against main."""
        self.git("switch", "-q", "-c", "pr")
        change(self)
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", "change")
        done = subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.root), "--base", "main"],
            capture_output=True,
            text=True,
            check=False,
        )
        return Outcome(done.returncode, done.stdout, done.stderr)


@pytest.fixture
def repo(tmp_path: Path) -> Repo:
    root = tmp_path / "repo"
    shutil.copytree(BASE_FIXTURE, root)
    result = Repo(root)
    result.git("init", "-q", "-b", "main")
    result.git("add", "-A")
    result.git("commit", "-q", "-m", "base")
    return result
