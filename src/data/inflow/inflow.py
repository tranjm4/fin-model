"""
File: data/inflow.py

This file handles the inflow of data into the system.
It reads data from yfinance's streaming API and sends it to a Kafka topic.
"""

import yfinance as yf
from src.data.mq.mq import KafkaProducerWrapper
from typing import List, Any, Optional, Dict

from tenacity import retry, stop_after_attempt, wait_fixed

from datetime import datetime

from dotenv import load_dotenv
import os
load_dotenv()

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TickStatus:
    """
    Represents a single stock ticker. Keeps track of its last read timestamp.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.last_read: Optional[datetime] = None

class TickerReader:
    """
    Reads stock ticker data from yfinance live streams.
    Provides recovery from the last read timestamp should errors occur
    """
    def __init__(self, kafka_producer: Optional[KafkaProducerWrapper] = None):
        if kafka_producer is None:
            self.kafka_producer = KafkaProducerWrapper(topic=os.getenv("KAFKA_TOPIC"))
        else:
            self.kafka_producer = kafka_producer
        self.symbols = self._get_tickers()
        self.tickers = yf.Tickers(self.symbols)
        self.last_read = {symbol: TickStatus(symbol) for symbol in self.symbols}

        logger.info(f"Initialized TickerReader with tickers: {self.symbols}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def start(self):
        # Start the yfinance streaming API
        try:
            logger.info("Starting ticker stream...")
            self.tickers.live(message_handler=self.message_handler, verbose=False)
        except Exception as e:
            logger.error(f"Error in ticker stream: {e}")
            self.set_last_read()

    def message_handler(self, message: Any):
        """Handles incoming messages from the yfinance API.
        """
        # Update the last read timestamp
        symbol = message.get('symbol')
        self.last_read[symbol].last_read = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.kafka_producer.send(message)

    def _get_tickers(self, file_name: str = "tickers.txt") -> List[str]:
        """Retrieves a predefined set of tickers to track from tickers.txt
        """
        tickers = []
        with open(file_name, "r") as f:
            tickers = [line.strip() for line in f.readlines()]
        return tickers

    def set_last_read(self):
        """
        Iterates through the last_read dictionary and writes the last read timestamps to recovery.txt
        """
        with open("recovery.txt", "w") as f:
            for symbol, status in self.last_read.items():
                f.write(f"{symbol}:{status.last_read}\n")

    async def recover_tickers(self):
        """
        Queries yfinance for the lost data given the last read from recovery.txt
        Uses current time as the end time for the query.
        """
        try:
            current_time = datetime.now()
            with open("recovery.txt", "r") as f:
                last_read = f.read().strip()
            logger.info(f"Recovering tickers from last read: {last_read}")
            # Query yfinance for the lost data
            self.tickers.history(period="1s", start=last_read)
        except Exception as e:
            logger.error(f"Error recovering tickers: {e}")