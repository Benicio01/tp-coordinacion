import pika
import threading
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from .middleware import (
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)
from pika.exceptions import AMQPConnectionError, AMQPError


class _MessageMiddlewareRabbitMQBase:
    def __init__(self, host, exchange, routing_keys):
        self.host = host
        self.exchange = exchange
        self.routing_keys = routing_keys
        self._consuming = False
        self.queue_name = None
        self._publish_lock = threading.Lock()
        try:
            self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.chan = self.conn.channel()
            self._declare_topology()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def _declare_topology(self):
        """Cada subclase define cómo arma su queue/exchange."""
        raise NotImplementedError

    def send(self, message):
        try:
            with self._publish_lock:
                for routing_key in self.routing_keys:
                    self.chan.basic_publish(
                        exchange=self.exchange, routing_key=routing_key, body=message
                    )
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def start_consuming(self, on_message_callback):
        def _pika_callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try:
            self._consuming = True
            self.chan.basic_consume(
                queue=self.queue_name,
                on_message_callback=_pika_callback,
                auto_ack=False,
            )
            self.chan.start_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError

    def stop_consuming(self):
        if not self._consuming:
            return
        try:
            self.chan.stop_consuming()
        except AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError
        except AMQPError:
            raise MessageMiddlewareMessageError
        finally:
            self._consuming = False

    def close(self):
        try:
            self.chan.close()
            self.conn.close()
        except AMQPError:
            raise MessageMiddlewareCloseError


class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        super().__init__(host, exchange="", routing_keys=[queue_name])
        self.queue_name = queue_name

    def _declare_topology(self):
        self.chan.queue_declare(queue=self.routing_keys[0], durable=True)


class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host, exchange=exchange_name, routing_keys=routing_keys)

    def _declare_topology(self):
        self.chan.exchange_declare(
            exchange=self.exchange, exchange_type="direct", durable=True
        )
        result = self.chan.queue_declare(queue="", exclusive=True)
        self.queue_name = result.method.queue
        for routing_key in self.routing_keys:
            self.chan.queue_bind(
                exchange=self.exchange,
                queue=self.queue_name,
                routing_key=routing_key,
            )