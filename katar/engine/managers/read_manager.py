from bisect import bisect_right
from pathlib import Path

from katar.engine.interactor.index_interactor import IndexInteractor
from katar.engine.interactor.logs_interactor import LogsInteractor
from katar.engine.managers.base_manager import BaseManager
from katar.engine.metadata import Metadata, MetadataKeys
from katar.engine.serializers import BaseSerializer
from katar.logger import logger


class ReadManager(BaseManager):
    def __init__(self, metadata: Metadata) -> None:
        super().__init__(metadata=metadata)

    def setup(self):
        self.update_base_offsets()
        self.update_live_segment()
        return self

    def _find_base_offset(self, offset):
        if offset >= self.base_offsets[-1]:
            self.base_offset = self.base_offsets[-1]
        else:
            base_offset_plusone = bisect_right(self.base_offsets, offset)
            self.base_offset = self.base_offsets[base_offset_plusone - 1]

    def read(self, offset):
        self._find_base_offset(offset)
        self.update_live_segment()

        location = self.index_interactor.find_location(offset, self.base_offset)

        bytes = self.logs_interactor.fetch_bytes(
            offset, self.base_offset, start=location
        )

        return bytes
