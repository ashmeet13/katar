import json
import time
import traceback
from pathlib import Path

from katar.engine.indexfile import IndexFile
from katar.engine.logsfile import LogsFile
from katar.engine.managers.read_controller import ReadController
from katar.engine.managers.write_controller import WriteController
from katar.logger import logger
from katar.settings import KATAR_DIR

# from katar.engine.managers.write_manager import ReadManager


class Topic:
    def __init__(
        self, topicname, max_segment_size=2 ** 30, index_byte_gap=1024
    ) -> None:
        super().__init__()
        self.topicname = topicname
        self.segments = []
        self.segments_count = 0
        self.max_segment_size = max_segment_size
        self.index_byte_gap = index_byte_gap

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

    def _get_metadata(self):
        return {
            "max_segment_size": str(self.max_segment_size),
            "index_byte_gap": str(self.index_byte_gap),
        }

    def _build_metadata(self, metadata):
        self.max_segment_size = int(metadata["max_segment_size"])
        self.index_byte_gap = int(metadata["index_byte_gap"])

    def _create_metadata_file(self):
        err = False
        try:
            with open(self.metadata_file_path, "w") as outfile:
                json.dump(self._get_metadata(), outfile)
        except Exception as e:
            message = "Failed to create topic folder"
            trace = traceback.format_exc()
            error = str(e)
            logger.error(event=message, error=error, stacktrace=trace)
            err = True
        return err

    def _read_metadata_file(self):
        err = False
        try:
            with open(self.metadata_file_path, "r") as outfile:
                metadata = json.load(outfile)
            self._build_metadata(metadata)
        except Exception as e:
            message = "Failed to create topic folder"
            trace = traceback.format_exc()
            error = str(e)
            logger.error(event=message, error=error, stacktrace=trace)
            err = True
        return err

    def initalise(self):
        self.topic_dir_path: Path = KATAR_DIR / self.topicname
        self.metadata_file_path = self.topic_dir_path / "metadata.json"
        if not self.topic_dir_path.is_dir():
            err = self._create_topic_folder(KATAR_DIR / self.topicname)
            if err:
                return
            err = self._create_metadata_file()
            if err:
                return
        else:
            err = self._read_metadata_file()
            if err:
                return

        self.write_controller = WriteController(
            topic_dir_path=self.topic_dir_path,
            max_segment_size=self.max_segment_size,
            index_byte_gap=self.index_byte_gap,
        )
        self.write_controller.setup()

        self.read_controller = ReadController(
            topic_dir_path=self.topic_dir_path,
            max_segment_size=self.max_segment_size,
            index_byte_gap=self.index_byte_gap,
        )

        self.read_controller.setup()

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
