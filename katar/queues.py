from collections import deque
from .exceptions import TopicAlreadyExists, TopicDoesNotExist


class Topics(object):
    topics = {}

    @classmethod
    def add(cls, topic_name, max_size=10):
        if cls.topics.get(topic_name, None):
            raise TopicAlreadyExists
        cls.topics[topic_name] = Queue(max_size)

    @classmethod
    def enqueue(cls, topic_name, item):
        if cls.topics.get(topic_name, None) is None:
            raise TopicDoesNotExist
        return cls.topics[topic_name].enqueue(item)

    @classmethod
    def dequeue(cls, topic_name):
        if cls.topics.get(topic_name, None) is None:
            raise TopicDoesNotExist
        return cls.topics[topic_name].dequeue()


class Queue():

    def __init__(self, max_size = 10):
        self._queue = deque(maxlen=max_size)

    def enqueue(self, item):
        self._queue.append(item)

    def dequeue(self):
        try:
            return self._queue.popleft()
        except:
            return None