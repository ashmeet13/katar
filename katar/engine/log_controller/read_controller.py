from bisect import bisect_right
from pathlib import Path

from katar.engine.io.index_interactor import IndexInteractor
from katar.engine.io.katar_interactor import KatarInteractor
from katar.engine.io.timeindex_interactor import TimeindexInteractor
from katar.engine.metadata import Metadata
from katar.engine.serializers import BaseSerializer
from katar.logger import logger


class ReadController:
    def __init__(self, topic_dir_path, metadata: Metadata) -> None:
        self.topic_dir_path: Path = topic_dir_path

        self.max_segment_size: int = metadata.max_segment_size
        self.index_byte_gap: int = metadata.index_byte_gap
        self.serializer: BaseSerializer = metadata.serializer

        self.katar_interactor = KatarInteractor()
        self.index_interactor = IndexInteractor()
        self.timeindex_interactor = TimeindexInteractor()

        self.base_offset = 0
        self.offset = 0

    def setup(self):
        self.base_offsets = self.katar_interactor.all_segments(self.topic_dir_path)

    def _find_base_offset(self, offset):
        if offset >= self.base_offsets[-1]:
            self.base_offset = self.base_offsets[-1]
        else:
            base_offset_plusone = bisect_right(self.base_offsets, offset)
            self.base_offset = self.base_offsets[base_offset_plusone - 1]

    def track_segment(self):
        self.segment_size = self.katar_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path, base_offset=self.base_offset
        )
        self.index_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path, base_offset=self.base_offset
        )
        self.timeindex_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path, base_offset=self.base_offset
        )

    def read(self, offset):
        self._find_base_offset(offset)
        self.track_segment()

        location = self.index_interactor.find_location(offset, self.base_offset)

        log = self.katar_interactor.get(
            offset, self.base_offset, serializer=self.serializer, start=location
        )

        return log
