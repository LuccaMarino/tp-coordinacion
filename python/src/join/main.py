import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        
        self.fruit_items_by_client = {}
        self.received_tops_by_client = {}   # cant de tops recibidos por cliente de los aggregations

        self._closed = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("(JOIN) Received SIGTERM signal")
        self._closed = True
        self.input_queue.stop_consuming()
        
    def _close(self):
        for connection in [self.input_queue, self.output_queue]:
            try:
                connection.close()
            except middleware.MessageMiddlewareCloseError as e:
                logging.error(e)

    def _process_top(self, client_id, fruit_top):
        fruit_items = self.fruit_items_by_client.setdefault(client_id, [])
        for [fruit, amount] in fruit_top:
            fruit_items.append(fruit_item.FruitItem(fruit, amount))
        
        self.received_tops_by_client[client_id] = (
            self.received_tops_by_client.get(client_id, 0) + 1
        )
        if self.received_tops_by_client[client_id] == AGGREGATION_AMOUNT:
            self._send_fruit_top(client_id)
            
    def _send_fruit_top(self, client_id):
        # aca ya llegaron todos los tops de este cliente, podemos borrar el registro de tops recibidos
        del self.received_tops_by_client[client_id]
        fruit_top = self.fruit_items_by_client.pop(client_id, [])
        fruit_top.sort()
        fruit_top.reverse()
                
        fruit_top_message = []
        for final_fruit_item in fruit_top[:TOP_SIZE]:
            fruit_top_message.append([final_fruit_item.fruit, final_fruit_item.amount])
        
        self.output_queue.send(
            message_protocol.internal.serialize_top(client_id, fruit_top_message)
        )
        logging.info(f"Sent final top for client {client_id}")
    
    def process_message(self, message, ack, nack):
        msg_type, client_id, args = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.TOP:
            self._process_top(client_id, *args)
        ack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_message)
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
