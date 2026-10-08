import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
_JOBS_PATH = str(_ROOT / "src" / "spark" / "jobs")

if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)
if _JOBS_PATH not in sys.path:
    sys.path.insert(0, _JOBS_PATH)
