import json

from katar.engine.serializers.base_serializer import BaseSerializer


class JSONSerializer(BaseSerializer):
    def serialize(self, payload: dict) -> bytes:
        return json.dumps(payload).encode("utf-8")

    def deserialize(self, payload: bytes) -> dict:
        return json.loads(payload.decode("utf-8"))
