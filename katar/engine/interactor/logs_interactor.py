import json
from functools import partial
from pathlib import Path

from katar.constants import *
from katar.engine.interactor.base_interactor import BaseInteractor
from katar.engine.metadata import Metadata
from katar.engine.serializers import BaseSerializer
from katar.engine.utils import int_from_bytes, int_to_bytes
from katar.logger import logger


class LogsInteractor(BaseInteractor):
    def __init__(self, metadata: Metadata) -> None:
        super().__init__(metadata=metadata)
        self.interactor_type = InteractorTypes.Logs

    def fetch_base_offsets(self):
        base_offsets = sorted(
            [
                int(file.name.split(".")[0])
                for file in Path(self.topic_dir).rglob(f"*.{self.interactor_type}")
            ]
        )
        return base_offsets

    def get_current_offset(self):
        offset = 0
        with open(self.tracking_file, "rb") as log_reader:
            for chunk in iter(partial(log_reader.read, 4), b""):
                log_length = int_from_bytes(chunk)
                log_reader.read(log_length)
                offset += 1

        return offset

    def get_size_from_location(self, location):
        size = 0
        ignore_first = True
        with open(self.tracking_file, "rb") as log_reader:
            log_reader.seek(location)
            for chunk in iter(partial(log_reader.read, 4), b""):
                log_length = int_from_bytes(chunk)
                log_reader.read(log_length)
                if ignore_first:
                    ignore_first = False
                    continue
                else:
                    size += log_length
        return size

    # lel
    def debug_print(self):
        print("Printing contents of file -", self.tracking_file)
        with open(self.tracking_file, "rb") as reader:
            for chunk in iter(partial(reader.read, 4), b""):
                length = int_from_bytes(chunk)
                payload_bytes = reader.read(length)
                print(length, payload_bytes)

    def fetch_bytes(self, offset, base_offset, start_search_location=0):
        target_offset = offset - base_offset
        log_bytes = None
        with open(self.tracking_file, "rb") as log_reader:
            log_reader.seek(start_search_location)
            for chunk in iter(partial(log_reader.read, 4), b""):
                log_length = int_from_bytes(chunk)
                # TODO: Save in the pattern: Location,Offset,Payload
                # rather than Location,Dict{Offset, Payload}

                # Currently I need to deserialize the payload on server side
                # to read the offset from the payload. Since with introduction
                # with proto I am shifting to let the client handle serializing and
                # deserializing, this hard dependency should be removed!
                log_bytes = log_reader.read(log_length)
                log = json.loads(log_bytes.decode("utf-8"))
                if log["offset"] == target_offset:
                    break
        return log_bytes
