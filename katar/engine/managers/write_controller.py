import json
import time
import traceback
from pathlib import Path

from katar.engine.io.index_interactor import IndexInteractor
from katar.engine.io.katar_interactor import KatarInteractor
from katar.engine.io.timeindex_interactor import TimeindexInteractor
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger


class WriteController:
    def __init__(self, topic_dir_path, max_segment_size, index_byte_gap) -> None:
        self.topic_dir_path: Path = topic_dir_path
        self.max_segment_size: int = max_segment_size
        self.index_byte_gap = index_byte_gap
        self.base_offset = 0
        self.offset = 0

        self.katar_interactor = KatarInteractor()
        self.index_interactor = IndexInteractor()
        self.timeindex_interactor = TimeindexInteractor()

    def _set_current_segment(self):
        err = False
        self.base_offset = 0
        log_paths = list(Path(self.topic_dir_path).rglob("*.katar"))
        for path in log_paths:
            segment = int(path.name.split(".")[0])

            try:
                index_file = self.topic_dir_path / f"{segment}.index"
                assert index_file.is_file()
                timeindex_file = self.topic_dir_path / f"{segment}.timeindex"
                assert timeindex_file.is_file()
                if segment > self.base_offset:
                    self.base_offset = segment
            except Exception as e:
                message = "Segment file is missing"
                trace = traceback.format_exc()
                error = str(e)
                logger.error(event=message, error=error, stacktrace=trace)
                err = True

        return err

    """
    Change the system to take the last log index - then from there iterate
    over the katar log file to calculate how many bytes from that index have been added.

    That is my "log index tracker" - everytime a write request comes add to this. If this
    "tracker" crossed max limit - write the index and reset the tracker.
    """

    def setup(self):
        err = self._set_current_segment()
        if err:
            return

        self.track_segment()
        self.offset = self.katar_interactor.get_current_offset()
        self.last_index_location = self.index_interactor.get_last_location()
        self.current_index_gap = self.katar_interactor.get_size_from_location(
            self.last_index_location
        )

    def track_segment(self):
        self.segment_size = self.katar_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path,
            base_offset=self.base_offset,
            create_file=True,
        )
        self.index_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path,
            base_offset=self.base_offset,
            create_file=True,
        )
        self.timeindex_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path,
            base_offset=self.base_offset,
            create_file=True,
        )

    def _serialize_payload(self, payload, timestamp):
        log = {
            "base_offset": self.base_offset,
            "offset": self.offset,
            "timestamp": timestamp,
            "payload": payload,
        }
        log_in_bytes = json.dumps(log).encode("utf-8")
        log_length_in_bytes = int_to_bytes(value=len(log_in_bytes), byte_size=4)
        return log_length_in_bytes + log_in_bytes

    def _create_index(self, log_location):
        log_location_in_bytes = int_to_bytes(value=log_location, byte_size=4)
        offset_in_bytes = int_to_bytes(value=self.offset, byte_size=4)
        return offset_in_bytes + log_location_in_bytes

    def write(self, payload):
        timestamp = int(time.time())
        logger.info(event="Log Insertion Request", message=payload)
        log = self._serialize_payload(payload, timestamp)

        try:
            log_location, new_segment_size = self.katar_interactor.write(log, len(log))
            self.offset += 1
        except Exception as e:
            raise e

        self.current_index_gap += len(log)
        if self.current_index_gap > self.index_byte_gap:
            logger.info(event="Inserting index", offset=self.offset)
            index = self._create_index(log_location)
            self.index_interactor.write(index, len(index))
            self.last_index_location = new_segment_size
            self.current_index_gap = 0

        self.segment_size = new_segment_size
        if self.segment_size > self.max_segment_size:
            logger.info(event="Creating new segment")
            self.base_offset = self.base_offset + self.offset
            self.track_segment()
            self.offset = self.katar_interactor.get_current_offset()
            self.last_index_location = self.index_interactor.get_last_location()
            self.current_index_gap = self.katar_interactor.get_size_from_location(
                self.last_index_location
            )
