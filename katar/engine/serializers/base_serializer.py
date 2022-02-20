class BaseSerializer:
    def serialize(self, payload):
        raise NotImplementedError()

    def deserialize(self, payload):
        raise NotImplementedError()

    def __str__(self) -> str:
        """Returns the specific serializer class used as string
        Returns:
            str: Serializer class name
        """
        return self.__class__.__name__
