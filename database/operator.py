import struct

from utils import str_to_long


class Operator:
    def __init__(self, operator_id, start, end) :
        self.operator_id = operator_id
        self.start = start
        self.end = end

    def encode(self):
        raise NotImplementedError


class Read(Operator):
    def __init__(self, operator_id, start, end, key, from_tid, from_oid) :
        super().__init__(operator_id, start, end)
        self.key = key
        self.from_tid = from_tid
        self.from_oid = from_oid

    def encode(self):
        op_type = b'R'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        key = struct.pack('<Q', str_to_long(self.key))
        from_tid = struct.pack('<I', self.from_tid)
        from_oid = struct.pack('<I', self.from_oid)
        ret = op_type + op_id + start + end + key+from_tid+from_oid
        return ret


class Write(Operator):
    def __init__(self, operator_id, start, end, key) :
        super().__init__(operator_id, start, end)
        self.key = key

    def encode(self):
        op_type = b'W'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        # key = struct.pack('<I', self.key)
        key = struct.pack('<Q', str_to_long(self.key))
        ret = op_type + op_id + start + end + key
        return ret


class Begin(Operator):
    def __init__(self, operator_id, start, end) :
        super().__init__(operator_id, start, end)

    def encode(self):
        op_type = b'S'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        ret = op_type + op_id + start + end
        return ret


class Commit(Operator):
    def __init__(self, operator_id, start, end) :
        super().__init__(operator_id, start, end)

    def encode(self):
        op_type = b'C'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        ret = op_type + op_id + start + end
        return ret


class Abort(Operator):
    def __init__(self, operator_id, start, end) :
        super().__init__(operator_id, start, end)

    def encode(self):
        op_type = b'A'
        op_id = struct.pack('<I', self.operator_id)
        start = struct.pack('<Q', self.start)
        end = struct.pack('<Q', self.end)
        ret = op_type + op_id + start + end
        return ret


# test
# import time
# tA = time.time_ns()
# tB = time.time_ns()
# read = Read(1, tA, tB, 111, 111, 1112)
# bytes_abort = read.encode()
# print(bytes_abort[0:1].decode())
# print(struct.unpack('<I', bytes_abort[1:5]))
# print(struct.unpack('<Q', bytes_abort[5:13]))
# print(struct.unpack('<Q', bytes_abort[13:21]))
# print(struct.unpack('<I', bytes_abort[21:25]))
# print(struct.unpack('<I', bytes_abort[25:29]))
