import json
import traceback
from pathlib import Path
from typing import Text

from katar.constants import *
from katar.engine.managers.read_manager import ReadManager
from katar.engine.managers.write_manager import WriteManager
from katar.engine.metadata import Metadata
from katar.logger import logger
from katar.settings import KATAR_DIR


class Topic:
    def __init__(self, metadata: Metadata) -> None:
        """Initalises a new object to wrap writing and reading
        from a topic.

        Caching will be implemented here.

        Args:
            metadata (Metadata): Metadata object storing everything necessary
            for Read and Write to work.
        """
        self.metadata = metadata
        self.topic_dir: Path = KATAR_DIR / self.metadata[MetadataKeys.Topic]
        if self.topic_dir.is_dir():
            self._create_topic_directory()

        self.metadata.add(MetadataKeys.TopicPath, self.topic_dir)

        self.write_manager = WriteManager(metadata=self.metadata).setup()
        self.read_manager = ReadManager(metadata=self.metadata).setup()

    def _create_topic_directory(self):
        """Creates a direction if a directory for the requested
        topic does not exist. This would be in the case of a new topic.
        """
        Path(self.topic_dir).mkdir(parents=True, exist_ok=True)

    def append(self, payload: bytes):
        """Add a log to the topic storage

        Args:
            payload (bytes): Log payload as recieved from the client
        """
        self.write_manager.write(payload)

    def scan(self, offset: int) -> bytes:
        """Scan the topic for getting the requested data at the offset.
        Currently only supports search by offset.

        Time based search will be added.

        Args:
            offset (int): Offset of the required Log

        Returns:
            bytes: Read log in bytes
        """
        return self.read_manager.read(offset)
