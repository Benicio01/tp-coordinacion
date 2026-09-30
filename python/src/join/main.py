import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.partials_by_client: dict[int, list[list[tuple[str, int]]]] = {}
        self.seen_eof_by_client: set[int] = set()

    def _merge_tops(self, partials: list[list[tuple[str, int]]]) -> list[tuple[str, int]]:
        """Mergea N tops parciales por cliente: suma por fruta y ordena desc."""
        merged: dict[str, fruit_item.FruitItem] = {}
        for top in partials:
            for fruit, amount in top:
                old = merged.get(fruit, fruit_item.FruitItem(fruit, 0))
                merged[fruit] = old + fruit_item.FruitItem(fruit, int(amount))
        items = list(merged.values())
        items.sort(reverse=True)  
        top_final = [(fi.fruit, fi.amount) for fi in items[:TOP_SIZE]]
        return top_final

    def process_messsage(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            if not (isinstance(fields, list) and len(fields) == 2 and isinstance(fields[1], list)):
                logging.error(f"Unexpected Join message format: {fields}")
                ack()
                return
            client_id, top = fields
            client_id = int(client_id)
            logging.info(f"Received partial top client={client_id} len={len(top)}")
            should_flush = False
            if client_id in self.seen_eof_by_client:
                logging.info(f"Top already flushed for client={client_id}, ignoring")
                ack()
                return
            
            lst = self.partials_by_client.setdefault(client_id, [])
            lst.append(top)
            if len(lst) == AGGREGATION_AMOUNT:
                should_flush = True

            if should_flush:
                partials = self.partials_by_client.pop(client_id, [])
                self.seen_eof_by_client.add(client_id)
                merged = self._merge_tops(partials)
                logging.info(f"Merged top client={client_id} -> {merged}")
                self.output_queue.send(message_protocol.internal.serialize([client_id, merged]))
            ack()
        except Exception as e:
            logging.error(f"Error in Join process_messsage: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join = JoinFilter()

    def handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping...")
        try:
            join.input_queue.stop_consuming()
        except Exception as e:
            logging.warning(f"Ignoring stop error during shutdown: {e}")

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        join.start()
    finally:
        try:
            join.input_queue.close()
        except Exception as e:
            logging.warning(f"Ignoring close input error during shutdown: {e}")
        try:
            join.output_queue.close()
        except Exception as e:
            logging.warning(f"Ignoring close output error during shutdown: {e}")
        logging.info("Shutdown graceful OK")
    return 0

if __name__ == "__main__":
    main()
