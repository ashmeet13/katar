import os
from pathlib import Path

KATAR_DIR = os.getenv("KATAR_DIR", None)
KATAR_LOGS_PATH = os.getenv("KATAR_LOGS_PATH", None)

# Setup katar directory
if KATAR_DIR is None:
    KATAR_DIR = Path.home() / ".katar"
else:
    KATAR_DIR = Path(KATAR_DIR)

KATAR_DIR.mkdir(parents=True, exist_ok=True)

# Katar logs path is kept as string since
# loguru needs it to be a string variable
if KATAR_LOGS_PATH is None:
    KATAR_LOGS_PATH = KATAR_DIR / "katar.log"
    KATAR_LOGS_PATH = str(KATAR_LOGS_PATH.resolve())
