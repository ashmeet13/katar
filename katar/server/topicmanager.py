from collections import OrderedDict
from typing import Union

from katar.engine.topic import Metadata, Topic
from katar.settings import KATAR_DIR


class TopicManager:
    def __init__(self, capacity: int = 5) -> None:
        self.cache = OrderedDict()
        self.cache_capacity = capacity
        self.all_topics = [
            topic_dir.name for topic_dir in KATAR_DIR.iterdir() if topic_dir.is_dir()
        ]

    def add(self, topicname: str):
        topic = Topic(topicname=topicname)
        metadata = Metadata(topic.topic_dir_path)
        topic.initalise(metadata=metadata)
        self.cache[topicname] = (topic, metadata)
        self.cache.move_to_end(key=topicname)
        if len(self.cache) > self.cache_capacity:
            self.cache.popitem(last=False)

    def get(self, topicname: str) -> tuple[Topic, Metadata]:
        if topicname not in self.cache:
            self.add(topicname=topicname)
        self.cache.move_to_end(key=topicname)
        return self.cache[topicname]

    def create(self, topicname: str, metadata_config: Union[dict, None]):
        topic = Topic(topicname=topicname)
        if metadata_config is None:
            Metadata(topic_dir=topic.topic_dir_path)
        else:
            Metadata(topic_dir=topic.topic_dir_path, **metadata_config)
        self.all_topics.append(topicname)
        return True

    def reset_metadata(self, topicname: str, metadata_config: Union[dict, None]):
        topic = Topic(topicname=topicname)
        metadata = Metadata(topic_dir=topic.topic_dir_path)
        if metadata_config is None:
            metadata.reset()
        else:
            metadata.reset(**metadata_config)

        if topicname in self.cache:
            topic.initalise(metadata=metadata)
            self.cache[topic] = (topic, metadata)

    def update_metadata(self, topicname: str, metadata_config: Union[dict, None]):
        topic = Topic(topicname=topicname)
        metadata = Metadata(topic_dir=topic.topic_dir_path)
        metadata.update(**metadata_config)

        if topicname in self.cache:
            topic.initalise(metadata=metadata)
            self.cache[topic] = (topic, metadata)

    def create_topicname(self, topic):
        topicname = topic
        exist = True
        exist_addition = 0
        while exist:
            if topicname not in self.all_topics:
                exist = False
            else:
                topicname = f"{topic}__{exist_addition}"
                exist_addition += 1

        return topicname

    def get_topic_metadata(self, topicname):
        if topic not in self.all_topics:
            return None
        topic = Topic(topicname=topicname)
        metadata = Metadata(topic.topic_dir_path)
        return metadata.get_metadata()

    def publish_data(self, topicname, payload):
        topic, _ = self.get(topicname=topicname)
        topic.append(payload=payload)

    def read_data(self, topicname, offset):
        topic, _ = self.get(topicname=topicname)
        return topic.read(offset=offset)
