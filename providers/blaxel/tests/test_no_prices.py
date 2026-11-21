# SPDX-FileCopyrightText: BoxRouter, Inc.
# SPDX-License-Identifier: PolyForm-Shield-1.0.0
"""FA-12: prices live in sandbox-provider-pricing only; this package states none."""

import re
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "src"


def test_the_package_states_no_price() -> None:
    stated = re.compile(r"\bPrice\(|\bPRICE_[A-Z_]+\b|\bprices=(?!\(\)|\[\])")
    found = [
        f"{path.relative_to(PACKAGE)}:{number}: {line.strip()}"
        for path in sorted(PACKAGE.rglob("*.py"))
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if stated.search(line)
    ]
    assert found == []
