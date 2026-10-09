import os
import shutil
import sys
from pathlib import Path

# Auto-descubrimiento de Java en entornos locales si no está en PATH
_CANDIDATE_JAVA_HOMES = [
    Path.home() / ".local/share/PrismLauncher/java/java-runtime-epsilon",
    Path("/usr/lib/jvm/default"),
]
if not shutil.which("java"):
    for cand in _CANDIDATE_JAVA_HOMES:
        if (cand / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(cand)
            os.environ["PATH"] = f"{cand / 'bin'}:{os.environ.get('PATH', '')}"
            break

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


_ROOT = Path(__file__).resolve().parent.parent
_COMMON_PATH = str(_ROOT / "src" / "spark" / "common")
_JOBS_PATH = str(_ROOT / "src" / "spark" / "jobs")

if _COMMON_PATH not in sys.path:
    sys.path.insert(0, _COMMON_PATH)
if _JOBS_PATH not in sys.path:
    sys.path.insert(0, _JOBS_PATH)
