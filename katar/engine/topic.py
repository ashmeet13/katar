from katar.engine.indexfile import IndexFile
from katar.engine.logsfile import LogsFile
from katar.settings import KATAR_DIR


class Topic:
    def __init__(self, topicname, max_offset_bytes=4, max_log_bytes=4) -> None:
        self.topicname = topicname
        self.max_offset_bytes = max_offset_bytes
        self.max_log_bytes = max_log_bytes
        self.logsfile = LogsFile(
            topicname=topicname,
            dirpath=KATAR_DIR / self.topicname,
            max_log_bytes=self.max_log_bytes,
        )
        self.indexfile = IndexFile(
            topicname=topicname,
            dirpath=KATAR_DIR / self.topicname,
            max_offset_bytes=self.max_offset_bytes,
            max_log_bytes=self.max_log_bytes,
        )
        self.indexfile.init_file()
        self.indexfile.init_offset()

    def add(self, content):
        location = self.logsfile.insert_log(content.encode("utf-8"))
        print("Location", location)
        offset = self.indexfile.write_index(location)
        print("Offset", offset)

    def read(self):
        self.logsfile.read_log(0)
