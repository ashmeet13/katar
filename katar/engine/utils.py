def int_to_bytes(value: int, byte_size: int) -> bytes:
    return value.to_bytes(byte_size, "big")


def int_from_bytes(xbytes: bytes) -> int:
    return int.from_bytes(xbytes, "big")
