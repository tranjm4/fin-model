"""
File: data/mq/mq.py

This file details wrapper classes for Kafka Consumers and Producers.
"""

from kafka import KafkaConsumer, KafkaProducer
import json

from typing import List, Any, Dict, Optional

from dotenv import load_dotenv
import os
load_dotenv(".env")

class KafkaProducerWrapper(KafkaProducer):
    """
    Wrapper for KafkaProducer
    """
    def __init__(self, topic, bootstrap_servers=os.getenv("BOOTSTRAP_SERVER")):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        super().__init__(
            bootstrap_servers=self.bootstrap_servers,
            batch_size=32768,       # 32KB batch size
            linger_ms=500,          # 500ms linger
            buffer_memory=33554432  # 32MB
        )
        
    def send(self, message) -> None:
        """Sends a message to the Kafka topic.
        
        Args:
            message (Any): The data to send to the topic
        """
        super().send(self.topic, json.dumps(message).encode('utf-8'))


class KafkaConsumerWrapper(KafkaConsumer):
    """
    Wrapper for KafkaConsumer
    """
    def __init__(self, 
                 topic: str, 
                 bootstrap_servers=os.getenv("BOOTSTRAP_SERVER"), 
                 group_id: str|None = None,
                 max_poll_records: int|None = 100,
                 session_timeout_ms: int|None = 10000,  # 10 second timeout
                 heartbeat_interval_ms: int|None = 3000 # 3 second heartbeat
                 ):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.max_poll_records = max_poll_records
        self.session_timeout_ms = session_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        super().__init__(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_records=self.max_poll_records,
            session_timeout_ms=self.session_timeout_ms,
            heartbeat_interval_ms=self.heartbeat_interval_ms
        )

    def consume(self) -> None:
        """Consumes messages from the Kafka topic."""
        for message in self:
            print(f"Received message: {message.value}")
            
