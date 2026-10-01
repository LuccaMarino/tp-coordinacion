from common import message_protocol
import uuid

class MessageHandler:

    def __init__(self):
        self._client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize_data(
            self._client_id, fruit, amount
            )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize_eof(self._client_id)

    def deserialize_result_message(self, message):
        _, client_id, [fruit_top] = message_protocol.internal.deserialize(message)
        
        if client_id != self._client_id:
            return None
        return fruit_top
