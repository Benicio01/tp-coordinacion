import os
import logging
import signal
import threading
import zlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.id = ID
        self.lock = threading.Lock()
        self.input_thread = None
        self.seen_eof: set[int] = set()
        self.amount_by_fruit: dict[int, dict[str, fruit_item.FruitItem]] = {}
        self.total_messages_expected_per_client: dict[int, int] = {}  # client_id -> N total del gateway
        self.local_messages_received_count: dict[int, int] = {}  # client_id -> n_i local
        self.peer_message_counts_by_sum: dict[int, dict[int, int]] = {}  # client_id -> {sum_id -> n_j}

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["control"]
        )

        self.control_publisher = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, ["control"]
        )

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data client={client_id} fruit={fruit} amount={amount}")
        with self.lock:
            if client_id in self.seen_eof:
                logging.warning(f"Ignoring late data for already flushed client={client_id}")
                return
            per_client = self.amount_by_fruit.setdefault(client_id, {})
            old = per_client.get(fruit, fruit_item.FruitItem(fruit, 0))
            new = fruit_item.FruitItem(fruit, int(amount))
            per_client[fruit] = old + new
            self.local_messages_received_count[client_id] = self.local_messages_received_count.get(client_id, 0) + 1

    def _process_eof(self, client_id):
        logging.info(f"Process EOF client={client_id}")
        with self.lock:
            if client_id in self.seen_eof:
                logging.info(f"EOF already processed for client={client_id}, skipping")
                return
            self.seen_eof.add(client_id)
            per_client = self.amount_by_fruit.pop(client_id, {})

        logging.info(f"Sharding {len(per_client)} fruits for client={client_id}")
        for final_fruit_item in per_client.values():
            idx = zlib.crc32(f"{client_id}:{final_fruit_item.fruit}".encode()) % AGGREGATION_AMOUNT
            self.data_output_exchanges[idx].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

        logging.info(f"Broadcasting EOF for client={client_id} to {AGGREGATION_AMOUNT} aggregators")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id]))

    def _publish_count(self, client_id):
        """Publica COUNT [client_id, sum_id, n_i] por el exchange de control."""
        with self.lock:
            n_i = self.local_messages_received_count.get(client_id, 0)

            if client_id not in self.peer_message_counts_by_sum:
                self.peer_message_counts_by_sum[client_id] = {}
            self.peer_message_counts_by_sum[client_id][self.id] = n_i
        try:
            self.control_publisher.send(
                message_protocol.internal.serialize([client_id, self.id, n_i])
            )
            logging.info(f"Published COUNT client={client_id} sum={self.id} n={n_i}")
        except Exception as e:
            logging.exception(f"Error publishing COUNT for client={client_id}: {e}")

    def _try_flush(self, client_id):
        """Flushea solo si todos los Sums reportaron y sum(n_j)==N."""
        should_flush = False
        with self.lock:
            if client_id in self.seen_eof:
                return
            if client_id not in self.total_messages_expected_per_client:
                return
            peer_map = self.peer_message_counts_by_sum.get(client_id, {})
            if len(peer_map) < SUM_AMOUNT:
                logging.info(
                    f"Waiting for counts client={client_id} have={len(peer_map)}/{SUM_AMOUNT} expected={self.total_messages_expected_per_client[client_id]} "
                )
                return
            total = sum(peer_map.values())
            expected = self.total_messages_expected_per_client[client_id]
            if total != expected:
                logging.info(
                    f"Counts incomplete client={client_id} total={total} expected={expected}, waiting for late data"
                )
                return
            should_flush = True
        if should_flush:
            self._process_eof(client_id)
            with self.lock:
                self.total_messages_expected_per_client.pop(client_id, None)
                self.local_messages_received_count.pop(client_id, None)
                self.peer_message_counts_by_sum.pop(client_id, None)

    def process_data_message(self, message, ack, nack):
        """Consume de input_queue. Solo 1 Sum recibe cada EOF."""
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                client_id, fruit, amount = fields
                self._process_data(client_id, fruit, amount)

                needs_republish = False
                with self.lock:
                    if client_id in self.total_messages_expected_per_client and client_id not in self.seen_eof:
                        needs_republish = True
                if needs_republish:
                    self._publish_count(client_id)
                    self._try_flush(client_id)
            elif len(fields) == 2:
                client_id, total = fields
                logging.info(f"Received EOF from queue for client={client_id} total={total}, forwarding to control exchange")
                self.control_publisher.send(message_protocol.internal.serialize([client_id, int(total)]))
            else:
                logging.error(f"Unexpected message format: {fields}")
            ack()
        except Exception as e:
            logging.exception(f"Error in process_data_message: {e}")
            nack()

    def _on_eof_control(self, client_id, total):
        logging.info(f"Received control EOF client={client_id} total={total}")
        with self.lock:
            if client_id in self.seen_eof:
                logging.info(f"EOF already flushed for client={client_id}, ignoring control EOF")
                return
            self.total_messages_expected_per_client[client_id] = total
            if client_id not in self.peer_message_counts_by_sum:
                self.peer_message_counts_by_sum[client_id] = {}
        self._publish_count(client_id)
        self._try_flush(client_id)

    def _on_count(self, client_id, peer_id, n_j):
        logging.info(f"Received COUNT client={client_id} peer={peer_id} n={n_j}")
        with self.lock:
            if client_id in self.seen_eof:
                return
            if client_id not in self.peer_message_counts_by_sum:
                self.peer_message_counts_by_sum[client_id] = {}
            self.peer_message_counts_by_sum[client_id][peer_id] = n_j
        self._try_flush(client_id)

    def process_control_message(self, message, ack, nack):
        """Consume de SUM_CONTROL_EXCHANGE (1 copia por cada Sum por cliente). Distingue EOF [c,N] vs COUNT [c, sum_id, n]"""
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 2:
                self._on_eof_control(*fields)
            elif len(fields) == 3:
                self._on_count(*fields)
            else:
                logging.error(f"Unexpected control message format: {fields}")
            ack()
        except Exception as e:
            logging.exception(f"Error in process_control_message: {e}")
            nack()

    def start(self):
        self.input_thread = threading.Thread(
            target=self.input_queue.start_consuming,
            args=(self.process_data_message,),
            name="sum-input",
            daemon=False,
        )
        self.input_thread.start()
        try:
            self.control_exchange.start_consuming(self.process_control_message)
        finally:
            try:
                self.input_queue.stop_consuming_threadsafe()
            except Exception as e:
                logging.warning(f"Ignoring stop input error during shutdown: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping...")
        try:
            sum_filter.control_exchange.stop_consuming_threadsafe()
        except Exception as e:
            logging.warning(f"Ignoring stop control error during shutdown: {e}")
        try:
            sum_filter.input_queue.stop_consuming_threadsafe()
        except Exception as e:
            logging.warning(f"Ignoring stop input error during shutdown: {e}")

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        sum_filter.start()
    finally:
        try:
            if sum_filter.input_thread is not None:
                sum_filter.input_thread.join()
        except Exception as e:
            logging.warning(f"Ignoring join error during shutdown: {e}")
        for middleware in [
            sum_filter.input_queue,
            sum_filter.control_exchange,
            sum_filter.control_publisher,
            *sum_filter.data_output_exchanges,
        ]:
            try:
                middleware.close()
            except Exception as e:
                logging.warning(f"Ignoring close error during shutdown: {e}")
        logging.info("Shutdown graceful OK")
    return 0


if __name__ == "__main__":
    main()
