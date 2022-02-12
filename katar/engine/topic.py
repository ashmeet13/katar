import json
import traceback
from pathlib import Path

from katar.engine.managers.read_controller import ReadController
from katar.engine.managers.write_controller import WriteController
from katar.engine.metadata import Metadata
from katar.logger import logger
from katar.settings import KATAR_DIR

# from katar.engine.managers.write_manager import ReadManager


class Topic:
    def __init__(
        self, topicname, max_segment_size=2 ** 30, index_byte_gap=1024
    ) -> None:
        super().__init__()
        self.topicname = topicname

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
            topic_dir_path=self.topic_dir_path,
            max_segment_size=self.metadata.max_segment_size,
            index_byte_gap=self.metadata.index_byte_gap,
        )
        self.write_controller.setup()

    def setup_read_controller(self):
        self.read_controller = ReadController(
            topic_dir_path=self.topic_dir_path,
            max_segment_size=self.metadata.max_segment_size,
            index_byte_gap=self.metadata.index_byte_gap,
        )

        self.read_controller.setup()

    def initalise(self):
        self.topic_dir_path: Path = KATAR_DIR / self.topicname
        if not self.topic_dir_path.is_dir():
            err = self._create_topic_folder(KATAR_DIR / self.topicname)
            if err:
                return

        self.metadata = Metadata(self.topic_dir_path)
        self.setup_write_controller()
        self.setup_read_controller()

    def append(self, payload):
        self.write_controller.write(payload)

    def read(self, offset):
        import time

        st = time.time()
        log = self.read_controller.read(offset)
        et = time.time()
        assert log["data"] == offset

        return et - st

    def read_diff(self, offset):
        import time

        st = time.time()
        log = self.read_controller.read_diff(offset)
        et = time.time()
        assert log["data"] == offset

        return et - st
