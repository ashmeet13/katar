class BaseSerializer:
    def serialize(self, payload):
        raise NotImplementedError()

    def deserialize(self, payload):
        raise NotImplementedError()
