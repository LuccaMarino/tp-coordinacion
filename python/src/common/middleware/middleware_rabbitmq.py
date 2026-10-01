import pika
import random
import string
from pika.exceptions import AMQPError, AMQPConnectionError
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError
)

# Cant. máxima de mensajes sin ack que se entregan a un consumidor a la vez.
# Con valor 1, hasta que no confirme el último mensaje, no se le envía uno nuevo 
PREFETCH_COUNT = 1

# Exchange por defecto, que rutea el mensaje a la cola 
# con el mismo nombre que la routing_key
EXCHANGE_DEFAULT = ""

EXCHANGE_TYPE = "direct"

# Nombre vacío para declarar una cola (en exchange), se le asigna uno único
UNIQUE_QUEUE_NAME = ""

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ con {host}") from e
        
        try:
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=queue_name)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Se desconectó al declarar la cola: {queue_name}") from e
        except AMQPError as e:
            self._connection.close()
            raise MessageMiddlewareMessageError(f"No se pudo delcarar la cola: {queue_name}") from e

    # Adapta la firma de callback de pika a la de la interfaz (message, ack, nack)
    def _on_message(self, channel, method, properties, body):
        self._on_message_callback(
            body,
            lambda: channel.basic_ack(delivery_tag=method.delivery_tag),
            lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
        )

    def start_consuming(self, on_message_callback):
        self._on_message_callback = on_message_callback

        try:
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=self._on_message,
                auto_ack=False,
            )
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Se cayó la conexón consumiendo de la cola: {self._queue_name}") from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error consumiendo de la cola: {self._queue_name}") from e
        
    def stop_consuming(self):
        try:
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(f"Se cayó la conexión al dejar de consumir de la cola: {self._queue_name}") from e
    
    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange=EXCHANGE_DEFAULT,
                routing_key=self._queue_name,
                body=message
            )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Se cayó la conexión enviando a la cola: {self._queue_name}") from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error enviando a la cola: {self._queue_name}") from e
        
    def close(self):
        try:
            self._connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error cerrando la conexión con la cola: {self._queue_name}") from e



class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        try:
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(f"No se pudo conectar a RabbitMQ con {host}") from e

        try: 
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange=exchange_name, exchange_type=EXCHANGE_TYPE)
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Se desconectó al declarar el exchange: {exchange_name}") from e
        except AMQPError as e:
            self._connection.close()
            raise MessageMiddlewareMessageError(f"No se pudo declarar el exchange: {exchange_name}") from e
    
    # Adapta la firma de callback de pika a la de la interfaz (message, ack, nack)
    def _on_message(self, channel, method, properties, body):
        self._on_message_callback(
            body,
            lambda: channel.basic_ack(delivery_tag=method.delivery_tag),
            lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
        )
    
    def start_consuming(self, on_message_callback):
        self._on_message_callback = on_message_callback
        try:
            # cola propia de este consumidor, con nombre unico
            res = self._channel.queue_declare(queue=UNIQUE_QUEUE_NAME, exclusive=True)
            queue_name = res.method.queue
            
            # bindeamos la cola a las routing_keys antes de consumir
            for routing_key in self._routing_keys:
                self._channel.queue_bind(exchange=self._exchange_name, queue=queue_name, routing_key=routing_key)
            
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=self._on_message,
                auto_ack=False
            )
            self._channel.start_consuming()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Se cayó la conexión consumiendo del exchange: {self._exchange_name}") from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error consumiendo del exchange {self._exchange_name}")from e
        
    def stop_consuming(self):
        try:
            self._connection.add_callback_threadsafe(self._channel.stop_consuming)
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(f"Se cayó la conexión al dejar de consumir del exchange {self._exchange_name}") from e
        
    def send(self, message):
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message
                )
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Se cayó la conexión enviando al exchange: {self._exchange_name}") from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error enviando al exchange: {self._exchange_name}") from e
    
    def close(self):
        try:
            self._connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error cerrando la conexión con el exchange: {self._exchange_name}") from e
        