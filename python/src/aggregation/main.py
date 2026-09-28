import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
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
        self.amounts_by_client: dict[int, dict[str, fruit_item.FruitItem]] = {}
        self.eof_counts_by_client: dict[int, int] = {}
        self.seen_eof_by_client: set[int] = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data client={client_id} fruit={fruit} amount={amount}")

        if client_id in self.seen_eof_by_client:
            logging.warning(f"Ignoring late data for flushed client={client_id}")
            return
        per_client = self.amounts_by_client.setdefault(client_id, {})
        old = per_client.get(fruit, fruit_item.FruitItem(fruit, 0))
        per_client[fruit] = old + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF client={client_id}")

        if client_id in self.seen_eof_by_client:
            logging.info(f"EOF already flushed for client={client_id}, ignoring")
            return
        cnt = self.eof_counts_by_client.get(client_id, 0) + 1
        self.eof_counts_by_client[client_id] = cnt
        if cnt < SUM_AMOUNT:
            logging.info(f"Waiting EOFs client={client_id} {cnt}/{SUM_AMOUNT}")
            return
        self.seen_eof_by_client.add(client_id)
        per_client = self.amounts_by_client.pop(client_id, {})
        self.eof_counts_by_client.pop(client_id, None)

        items = list(per_client.values())
        items.sort(reverse=True)
        top = [(fi.fruit, fi.amount) for fi in items[:TOP_SIZE]]
        
        self.output_queue.send(message_protocol.internal.serialize([client_id, top]))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        try:
            fields = message_protocol.internal.deserialize(message)
            if len(fields) == 3:
                client_id, fruit, amount = fields
                self._process_data(client_id, fruit, amount)
            elif len(fields) == 1:
                (client_id,) = fields
                self._process_eof(client_id)
            else:
                logging.error(f"Unexpected message format: {fields}")
        except Exception as e:
            logging.error(f"Error in process_messsage: {e}")
            nack()
            return
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    agg = AggregationFilter()

    def handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping...")
        try:
            agg.input_exchange.stop_consuming()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        agg.start()  
    finally:
        try: agg.input_exchange.close()
        except Exception: pass
        try: agg.output_queue.close()
        except Exception: pass
        logging.info("Shutdown graceful OK")
    return 0


if __name__ == "__main__":
    main()
