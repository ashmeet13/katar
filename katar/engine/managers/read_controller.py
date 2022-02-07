import json
import time
import traceback
from bisect import bisect_right
from pathlib import Path

from katar.engine.io.index_interactor import IndexInteractor
from katar.engine.io.katar_interactor import KatarInteractor
from katar.engine.io.timeindex_interactor import TimeindexInteractor
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger

# def _track_log_segments(self):
#     log_paths = list(Path(self.topic_dir_path).rglob("*.katar"))

#     err = False
#     for path in log_paths:
#         segment = path.name.split(".")[0]

#         try:
#             index_file = self.topic_dir_path / f"{segment}.index"
#             timeindex_file = self.topic_dir_path / f"{segment}.timeindex"
#             assert index_file.is_file()
#             assert timeindex_file.is_file()
#         except Exception as e:
#             message = "Segment file is missing"
#             trace = traceback.format_exc()
#             error = str(e)
#             logger.error(event=message, error=error, stacktrace=trace)
#             err = True
#             return err

#         self.segments.append(int(segment))
#     self.segments_count = len(self.segments)
#     success = True
#     return success


class ReadController:
    def __init__(self, topic_dir_path, max_segment_size, index_byte_gap) -> None:
        self.topic_dir_path: Path = topic_dir_path
        self.max_segment_size: int = max_segment_size
        self.index_byte_gap = index_byte_gap
        self.base_offset = 0
        self.offset = 0

        self.katar_interactor = KatarInteractor()
        self.index_interactor = IndexInteractor()
        self.timeindex_interactor = TimeindexInteractor()

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

        self.index_interactor.print()

        location = self.index_interactor.find_location(offset, self.base_offset)
        print("Got Location -", location)
