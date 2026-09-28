from common import message_protocol
import threading

class MessageHandler:
    _id_counter = 0
    _lock = threading.Lock()

    def __init__(self):
        with MessageHandler._lock:
            MessageHandler._id_counter += 1
            self.client_id = MessageHandler._id_counter
        self._seq = 0

    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        self._seq += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self._seq])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if self.client_id != fields[0]:
            return None
        return fields[1]
