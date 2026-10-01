import json

class MsgType:
    DATA = 1
    EOF = 2
    TOP = 3


def _serialize(fields):
    return json.dumps(fields).encode("utf-8")

def serialize_data(client_id, fruit, amount):
    return _serialize([MsgType.DATA, client_id, fruit, amount])

def serialize_eof(client_id):
    return _serialize([MsgType.EOF, client_id])

def serialize_top(client_id, fruit_top):
    return _serialize([MsgType.TOP, client_id, fruit_top])

def deserialize(message):
    [msg_type, client_id, *args] = json.loads(message.decode("utf-8"))
    return msg_type, client_id, args
