import json
from pathlib import Path
from typing import Any, Text

from katar.logger import logger


class Metadata(dict):
    def __init__(self, topic_dir: Path) -> None:
        """Every topic requires a lot of variables to be passed between
        functions. This acts a wrapper to keep them concise. Every entry
        in dictionary is has consistent key saved in the constants file

        Args:
            topic_dir (Path): Path where the topic data is stored
        """
        super().__init__()
        self.metadata_path = topic_dir / "metadata.json"

    def load(self):
        """Load from metadata.json"""
        with open(self.metadata_path, "r") as readfile:
            self.metadata = json.load(readfile)

    def save(self):
        """Save to metadata.json"""
        with open(self.metadata_path, "w") as outfile:
            json.dump(self.metadata, outfile)
