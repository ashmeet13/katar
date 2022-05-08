import shutil
from pathlib import Path

import pytest

from katar.constants import MetadataKeys
from katar.engine.interactor.base_interactor import BaseInteractor
from katar.engine.metadata import Metadata


class TestBaseInteractor:
    @classmethod
    def setup_class(cls):
        cls.testTopic = "test_topic"
        cls.tempDirPath = Path.cwd() / "tmp"
        cls.testTopicPath = cls.tempDirPath / cls.testTopic

        cls.metadata = Metadata(cls.tempDirPath)
        cls.metadata[MetadataKeys.Topic] = cls.testTopic
        cls.metadata[MetadataKeys.TopicPath] = cls.testTopicPath

        Path(cls.tempDirPath).mkdir(parents=True, exist_ok=True)

    @classmethod
    def teardown_class(cls):
        if cls.tempDirPath.exists():
            shutil.rmtree(cls.tempDirPath)

    def setup_method(self, method):
        if self.testTopicPath.exists():
            shutil.rmtree(self.testTopicPath)
        Path(self.testTopicPath).mkdir(parents=True, exist_ok=True)

    def teardown_method(self, method):
        if self.testTopicPath.exists():
            shutil.rmtree(self.testTopicPath)

    def test_set_tracking_file_with_create(self):
        interactor = BaseInteractor(self.metadata)

        interactor.set_tracking_file(0, create_file=True)
        interactor.set_tracking_file(1, create_file=True)
        interactor.set_tracking_file(2, create_file=True)

        for file in self.testTopicPath.iterdir():
            assert file.name in ["0.base", "1.base", "2.base"]

    def test_fetch_total_file_count(self):
        interactor = BaseInteractor(self.metadata)

        interactor.set_tracking_file(0, create_file=True)
        interactor.set_tracking_file(1, create_file=True)
        interactor.set_tracking_file(2, create_file=True)

        file_count = interactor.fetch_total_file_count()
        assert file_count == 3

    def test_set_tracking_file_without_create(self):
        interactor = BaseInteractor(self.metadata)

        with open(self.testTopicPath / "16.base", "wb") as writer:
            writer.write(b"0123456789")

        interactor.set_tracking_file(16)
        tracking_file_size = interactor.fetch_file_size()

        assert tracking_file_size == 10

    def test_file_exists_and_create(self):
        interactor = BaseInteractor(self.metadata)

        file_exists = interactor.file_exists(base_offset=8)
        assert file_exists == False

        with open(self.testTopicPath / "8.base", "wb") as writer:
            pass

        file_exists = interactor.file_exists(base_offset=8)
        assert file_exists == True

    def test_write_new_file(self):
        interactor = BaseInteractor(self.metadata)
        interactor.set_tracking_file(0, create_file=True)

        log_text = b"This is a test"
        log_size = len(log_text)
        interactor.write(log_text, log_size)

        tracking_file_size = interactor.fetch_file_size()
        assert tracking_file_size == log_size

        with open(self.testTopicPath / "0.base", "rb") as reader:
            log = reader.read()

        assert log == log_text

    def test_write_existing_file(self):
        log_text = b"This is a test"
        log_size = len(log_text)
        with open(self.testTopicPath / "0.base", "wb") as writer:
            writer.write(log_text)

        interactor = BaseInteractor(self.metadata)
        interactor.set_tracking_file(0, create_file=True)

        interactor.write(log_text, log_size)

        tracking_file_size = interactor.fetch_file_size()
        assert tracking_file_size == 2 * log_size

        with open(self.testTopicPath / "0.base", "rb") as reader:
            log = reader.read()

        assert log == log_text + log_text
