from loguru import logger

from katar.settings import KATAR_LOGS_PATH

logger.add(KATAR_LOGS_PATH)
