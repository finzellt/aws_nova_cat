from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Allow tests to import from infra/ as a package root
# (nova_cat, nova_constructs live there)
sys.path.insert(0, str(Path(__file__).parent / "infra"))

# Service handlers
sys.path.insert(0, str(REPO_ROOT / "services"))
