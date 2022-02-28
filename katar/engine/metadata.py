import json
import traceback
from pathlib import Path
from typing import Union

from katar.engine.serializers import BaseSerializer, serializers
from katar.logger import logger


class Metadata:
    def __init__(
        self,
        topic_dir: Path,
        max_segment_size: int = 2 ** 30,
        index_byte_gap: int = 1024,
        serializer: Union[BaseSerializer, str] = serializers["JSONSerializer"](),
    ) -> None:
        self.metadata_path = topic_dir / "metadata.json"

        if self._metadata_file_exists():
            logger.info(event="Loading metadata from file")
            self.read_from_file()
        else:
            logger.info(event="Creating metadata dictionary")
            self.set_parameters(max_segment_size, index_byte_gap, serializer)
            self.save()

    def set_parameters(self, max_segment_size, index_byte_gap, serializer):
        self.max_segment_size = max_segment_size
        self.index_byte_gap = index_byte_gap

        if type(serializer) == str:
            self.serializer = serializers[serializer]()
        else:
            self.serializer = serializer

    def get_metadata(self):
        return {
            "max_segment_size": str(self.max_segment_size),
            "index_byte_gap": str(self.index_byte_gap),
            "serializer": str(self.serializer),
        }

    def _build_metadata(self, metadata):
        self.max_segment_size = int(metadata["max_segment_size"])
        self.index_byte_gap = int(metadata["index_byte_gap"])
        self.serializer = serializers[metadata["serializer"]]()

    def _metadata_file_exists(self):
        return self.metadata_path.is_file()

    def save(self):
        err = False
        try:
            with open(self.metadata_path, "w") as outfile:
                json.dump(self.get_metadata(), outfile)
        except Exception as e:
            message = "Failed to create topic folder"
            trace = traceback.format_exc()
            error = str(e)
            logger.error(event=message, error=error, stacktrace=trace)
            err = True
        return err

    def read_from_file(self):
        err = False
        try:
            with open(self.metadata_path, "r") as readfile:
                metadata = json.load(readfile)
            self._build_metadata(metadata)
        except Exception as e:
            message = "Failed to create topic folder"
            trace = traceback.format_exc()
            error = str(e)
            logger.error(event=message, error=error, stacktrace=trace)
            err = True
        return err

    def reset(
        self,
        max_segment_size: int = 2 ** 30,
        index_byte_gap: int = 1024,
        serializer: Union[BaseSerializer, str] = serializers["JSONSerializer"](),
    ) -> None:
        self.max_segment_size = max_segment_size
        self.index_byte_gap = index_byte_gap
        if type(serializer) == str:
            self.serializer = serializers[serializer]()
        else:
            self.serializer = serializer
        self.save()

    def update(self, max_segment_size=None, index_byte_gap=None, serializer=None):
        if max_segment_size:
            self.max_segment_size = max_segment_size

        if index_byte_gap:
            self.index_byte_gap = index_byte_gap

        if serializer:
            self.serializer = serializer

        self.save()

    def delete(self):
        self.metadata_path.unlink(missing_ok=True)
