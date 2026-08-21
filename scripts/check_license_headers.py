# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Fail if any source file under src/ lacks the standard license header.

Run from the repository root (as .github/workflows/license-headers.yml does):

    python scripts/check_license_headers.py

Every .py file under src/ must begin with exactly:

    # Copyright © <year>, UChicago Argonne, LLC
    # See LICENSE for terms and disclaimer.
"""

import re
import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
_LINE_1 = re.compile(r"# Copyright © \d{4}, UChicago Argonne, LLC")
_LINE_2 = "# See LICENSE for terms and disclaimer."


def main() -> int:
    offenders: list[str] = []
    for py_file in sorted(_SRC.rglob("*.py")):
        lines = py_file.read_text(encoding="utf-8").splitlines()
        first_two = lines[:2] + [""] * (2 - len(lines))
        if not (_LINE_1.fullmatch(first_two[0]) and first_two[1] == _LINE_2):
            offenders.append(str(py_file.relative_to(_SRC.parent)))

    if offenders:
        print("Missing or malformed license header (expected on lines 1-2):")
        for path in offenders:
            print(f"  {path}")
        return 1

    print(f"License headers OK ({sum(1 for _ in _SRC.rglob('*.py'))} files).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
