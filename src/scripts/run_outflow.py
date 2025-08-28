from src.data.outflow.outflow import TickerReader
from src.data.mq.mq import KafkaProducerWrapper

if __name__ == "__main__":
    kafka_producer = KafkaProducerWrapper()
    ticker_reader = TickerReader(kafka_producer)
    ticker_reader.start()
