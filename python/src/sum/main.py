import os
import logging
import threading
import zlib
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

# Determina el indice del Aggregation que va a encargarse de la fruta recibida.
# El hash crc32 es deterministico
def _aggregation_id(fruit):
    return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        # control_consumer_exchange y control_publisher_exchange son el mismo exchange
        # para coordinar los sums, pero 2 objetos porque se usan en 2 hilos distintos,
        # ya que uno se queda bloqueado en start_consuming()
        # control_consumer_exchange se usa para consumir (en hilo aparte), 
        # control_publisher_exchange para publicar
        self.control_consumer_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
        )
        self.control_publisher_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [SUM_PREFIX]
        )
        
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_client = {}
        ## protege amount_by_fruit_by_client entre los 2 hilos del Sum
        self.lock = threading.Lock()
        
        self._closed = False
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        logging.info("(SUM) Received SIGTERM signal")
        self._closed = True
        self.input_queue.stop_consuming()
        self.control_consumer_exchange.stop_consuming()
        
    def _close(self):
        connections = [
            self.input_queue,
            self.control_consumer_exchange,
            self.control_publisher_exchange,
        ] + self.data_output_exchanges
        
        for connection in connections:
            try:
                connection.close()
            except middleware.MessageMiddlewareCloseError as e:
                logging.error(e)
            

    def _process_data(self, client_id, fruit, amount):
        with self.lock:
            amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)

    # Solo publica el EOF en el exchange de control
    def _process_eof(self, client_id):
        logging.info(f"Client {client_id} finished sending records")
        self.control_publisher_exchange.send(
            message_protocol.internal.serialize_eof(client_id)
        )

    # Envía los datos totales de un cliente a los Aggregations correspondientes
    def _flush_client(self, client_id):
        # toma el lock para sacar los datos del cliente que ya terminó
        with self.lock:
            amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        
        # los totales de cada fruta van a una unica Aggregation, según su hash
        for final_fruit_item in amount_by_fruit.values():
            data_output_exchange = self.data_output_exchanges[
                _aggregation_id(final_fruit_item.fruit)
            ]
            data_output_exchange.send(
                message_protocol.internal.serialize_data(
                    client_id, final_fruit_item.fruit, final_fruit_item.amount
                )
            )
        
        # manda el aviso de EOF del cliente a todos los Aggregation, aunque
        # algun Sum no haya recibido ninguna fruta de este cliente, ya que cada
        # Aggregation necesita SUM_AMOUNT avisos para saber que ese cliente terminó
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(
                message_protocol.internal.serialize_eof(client_id)
            )
        logging.info(f"Finished flushing client {client_id}")

    def process_input_message(self, message, ack, nack):
        msg_type, client_id, args = message_protocol.internal.deserialize(message)
        if msg_type == message_protocol.internal.MsgType.DATA:
            self._process_data(client_id, *args)
        elif msg_type == message_protocol.internal.MsgType.EOF:
            self._process_eof(client_id)
        ack()

    # Procesa los mensajes de control, que son los avisos de EOF. Da igual
    # si viene de una retransmision de esta misma replica, todas hacen lo mismo
    def process_control_message(self, message, ack, nack):
        _, client_id, _ = message_protocol.internal.deserialize(message)
        self._flush_client(client_id)
        ack()

    def start(self):
        # hilo que consume del exchange de control
        self.control_thread = threading.Thread(
            target=self.control_consumer_exchange.start_consuming,
            args=(self.process_control_message,),
        )
        self.control_thread.start()
        try:
            self.input_queue.start_consuming(self.process_input_message)
        finally:
            self.control_thread.join()
            self._close()
        

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
