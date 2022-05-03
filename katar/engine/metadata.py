import json
from pathlib import Path
from typing import Any, Text

from katar.logger import logger


class Metadata:
    def __init__(self, topic_dir: Text) -> None:
        self.metadata_path = topic_dir / "metadata.json"
        self.metadata = dict()

    def add(self, key: Text, value: Any):
        if key in self.metadata:
            return False
        self.metadata[key] = value
        return True

    def remove(self, key):
        if key not in self.metadata:
            return False
        del self.metadata[key]
        return True

    def update(self, key, value):
        if key not in self.metadata:
            return False
        self.metadata[key] = value
        return True

    def load(self):
        with open(self.metadata_path, "r") as readfile:
            self.metadata = json.load(readfile)

    def save(self):
        with open(self.metadata_path, "w") as outfile:
            json.dump(self.metadata, outfile)
