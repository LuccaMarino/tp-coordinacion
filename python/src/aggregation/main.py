import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_fruit_by_client = {}
        self.eofs_by_client = {}    # cant de eofs recibidos por cliente de los sums

        self._closed = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        
    def _handle_sigterm(self, signum, frame):
        logging.info("(AGGREGATION) Received SIGTERM signal")
        self._closed = True
        self.input_exchange.stop_consuming()
        
    def _close(self):
        for connection in [self.input_exchange, self.output_queue]:
            try:
                connection.close()
            except middleware.MessageMiddlewareCloseError as e:
                logging.error(e)
        
    def _process_data(self, client_id, fruit, amount):
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        self.eofs_by_client[client_id] = self.eofs_by_client.get(client_id, 0) + 1 
        # si recibió eof de todos los Sums de este cliente, manda el top al Join
        if self.eofs_by_client[client_id] == SUM_AMOUNT:
            self._send_fruit_top(client_id)

    def _send_fruit_top(self, client_id):
        # aca ya llegaron todos los eofs de este cliente, podemos borrar el registro de eofs
        del self.eofs_by_client[client_id]
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        fruit_top = sorted(amount_by_fruit.values())
        fruit_top.reverse()
        
        fruit_top_message = []
        for final_fruit_item in fruit_top[:TOP_SIZE]:
            fruit_top_message.append([final_fruit_item.fruit, final_fruit_item.amount])
        
        self.output_queue.send(
            message_protocol.internal.serialize_top(client_id, fruit_top_message)
        )
        logging.info(f"Sent partial top for client {client_id}")
        
    def process_message(self, message, ack, nack):
        msg_type, client_id, args = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.DATA:
            self._process_data(client_id, *args)
        elif msg_type == message_protocol.internal.MsgType.EOF:
            self._process_eof(client_id)
        ack()

    def start(self):
        try:
            self.input_exchange.start_consuming(self.process_message)
        finally:
            self._close()

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
