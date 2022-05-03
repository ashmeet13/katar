import json
import traceback
from pathlib import Path
from typing import Text

from katar.engine.managers.read_manager import ReadManager
from katar.engine.managers.write_manager import WriteManager
from katar.engine.metadata import Metadata, MetadataKeys
from katar.logger import logger
from katar.settings import KATAR_DIR


class Topic:
    def __init__(self, metadata: Metadata) -> None:
        self.metadata = metadata
        self.topic_dir: Path = KATAR_DIR / self.metadata[MetadataKeys.TOPIC]
        if self.topic_dir.is_dir():
            self._create_topic_directory()

        self.metadata.add(MetadataKeys.TOPICPATH, self.topic_dir)

        self.write_manager = WriteManager(metadata=self.metadata).setup()
        self.read_manager = ReadManager(metadata=self.metadata).setup()

    def _create_topic_directory(self):
        Path(self.topic_dir).mkdir(parents=True, exist_ok=True)

    def append(self, payload):
        self.write_manager.write(payload)

    def read(self, offset):
        return self.read_manager.read(offset)
