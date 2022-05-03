from pathlib import Path

from katar.constants import MetadataKeys
from katar.engine.interactor.index_interactor import IndexInteractor
from katar.engine.interactor.logs_interactor import LogsInteractor
from katar.engine.metadata import Metadata
from katar.logger import logger


class BaseManager:
    def __init__(self, metadata: Metadata) -> None:
        self.metadata = metadata

        self.topic_dir_path: Path = self.metadata[MetadataKeys.TopicPath]
        self.segment_size: int = self.metadata[MetadataKeys.SegmentSize]
        self.index_gap: int = self.metadata[MetadataKeys.IndexGap]

        self.logs_interactor = LogsInteractor(metadata=metadata)
        self.index_interactor = IndexInteractor(metadata=metadata)

        self.base_offset = 0
        self.total_file_counts = 0

    def update_base_offsets(self):
        total_file_counts = self.logs_interactor.fetch_total_file_count()
        if total_file_counts > self.total_file_counts:
            self.total_file_counts = total_file_counts
            self.base_offsets = self.logs_interactor.fetch_base_offsets()

    def update_live_segment(self, create_file):
        self.logs_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path,
            base_offset=self.base_offset,
            create_file=create_file,
        )
        self.index_interactor.set_tracking_file(
            topic_dir=self.topic_dir_path,
            base_offset=self.base_offset,
            create_file=create_file,
        )

        self.segment_logs_size = self.logs_interactor.fetch_file_size()
