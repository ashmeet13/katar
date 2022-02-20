from pathlib import Path

from katar.engine.io.base_interactor import BaseInteractor
from katar.logger import logger


class TimeindexInteractor(BaseInteractor):
    def __init__(self) -> None:
        super().__init__()
        self.interactor_type = "timeindex"
