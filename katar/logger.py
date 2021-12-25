import logging

import structlog

from katar.settings import KATAR_LOGS_PATH

logging.basicConfig(filename=KATAR_LOGS_PATH)
logger = structlog.get_logger("Katar")
