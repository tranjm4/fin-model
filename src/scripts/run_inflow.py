from src.data.inflow.inflow import TickerReader
from src.data.mq.mq import KafkaProducerWrapper

if __name__ == "__main__":
    ticker_reader = TickerReader()
    ticker_reader.start()