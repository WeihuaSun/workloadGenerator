from utils import *
import struct
hash_set = set()

for i in range(1, 10001):
    key = encode_key(i)
    packed_key = pack_key("blind", key)
    hash_val = hash_string_to_bytes(packed_key)
    val = struct.unpack("<I", hash_val)
    if val in hash_set:
        print("error")
    hash_set.add(val)

