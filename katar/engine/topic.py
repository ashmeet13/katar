import json
import traceback
from pathlib import Path

from katar.engine.log_controller.read_controller import ReadController
from katar.engine.log_controller.write_controller import WriteController
from katar.engine.metadata import Metadata
from katar.logger import logger
from katar.settings import KATAR_DIR

# from katar.engine.managers.write_manager import ReadManager


class Topic:
    def __init__(self, topicname) -> None:
        super().__init__()
        self.topicname = topicname

        self.topic_dir_path: Path = KATAR_DIR / self.topicname
        if not self.topic_dir_path.is_dir():
            err = self._create_topic_folder(KATAR_DIR / self.topicname)
            if err:
                return

    def _create_topic_folder(self, topic_dir_path):
        err = False

        try:
            Path(topic_dir_path).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            message = "Failed to create topic folder"
            trace = traceback.format_exc()
            error = str(e)
            logger.error(event=message, error=error, stacktrace=trace)
            err = True

        return err

    def setup_write_controller(self):
        self.write_controller = WriteController(
            topic_dir_path=self.topic_dir_path, metadata=self.metadata
        )
        self.write_controller.setup()

    def setup_read_controller(self):
        self.read_controller = ReadController(
            topic_dir_path=self.topic_dir_path, metadata=self.metadata
        )

        self.read_controller.setup()

    def initalise(self, metadata: Metadata):
        self.metadata = metadata
        self.setup_write_controller()
        self.setup_read_controller()

    def append(self, payload):
        self.write_controller.write(payload)

    def read(self, offset=None):
        log = self.read_controller.read(offset)
        return log
