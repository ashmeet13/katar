import json
import time
import traceback
from pathlib import Path

from katar.engine.interactor.index_interactor import IndexInteractor
from katar.engine.interactor.logs_interactor import LogsInteractor
from katar.engine.managers.base_manager import BaseManager
from katar.engine.metadata import Metadata, MetadataKeys
from katar.engine.serializers import BaseSerializer
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger


class WriteManager(BaseManager):
    def __init__(self, metadata: Metadata) -> None:
        super().__init__(metadata=metadata)
        self.write_offset = 0

    def setup(self):
        self.update_base_offsets()
        self.update_live_segment(create_file=True)
        self.setup_write_info()
        return self

    def setup_write_info(self):
        self.base_offset = self.base_offsets[-1]
        self.write_offset = self.logs_interactor.get_current_offset()
        self.last_index_location = self.index_interactor.get_last_location()
        self.current_index_gap = self.logs_interactor.get_size_from_location(
            self.last_index_location
        )

    def _serialize_index(self, log_location):
        log_location_in_bytes = int_to_bytes(value=log_location, byte_size=4)
        offset_in_bytes = int_to_bytes(value=self.write_offset, byte_size=4)
        return offset_in_bytes + log_location_in_bytes

    def write_log(self, payload, payload_size):
        log_location = self.logs_interactor.write(payload, payload_size)
        self.write_offset += 1
        self.segment_logs_size = self.logs_interactor.fetch_file_size()

        return log_location

    def write_index(self, log_location):
        index = self._serialize_index(log_location)
        self.index_interactor.write(index, len(index))
        self.last_index_location = self.segment_logs_size
        self.current_index_gap = 0

    def create_new_segment(self):
        self.base_offset = self.base_offset + self.write_offset
        logger.info(event=f"New Segment Base Offset = {self.base_offset}")
        self.update_live_segment(create_file=True)
        self.setup_write_info()

    def write(self, payload: bytes):
        logger.info(
            event="Inserting Log",
            message=payload,
            base_offset=self.base_offset,
            offset=self.write_offset,
        )
        payload_size = len(payload)
        log_location = self.write_log(payload=payload, payload_size=payload_size)

        self.current_index_gap += payload_size
        if self.current_index_gap > self.index_gap:
            logger.info(
                event="Inserting index",
                offset=self.write_offset - 1,
                last_index_location=self.last_index_location,
                current_index_gap=self.current_index_gap,
                index_byte_gap=self.index_gap,
            )
            self.write_index(log_location=log_location)

        if self.segment_logs_size > self.segment_size:
            logger.info(event="Creating new segment")
            self.create_new_segment()
