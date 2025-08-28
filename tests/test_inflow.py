from unittest.mock import Mock, patch, mock_open
from src.data.inflow.inflow import TickerReader
from src.data.mq.mq import KafkaProducerWrapper

def test_ticker_reader_init():
    """
    Tests the initialization of the TickerReader class.
    """
    # Mock the KafkaProducerWrapper to avoid actual Kafka connection
    mock_producer = Mock(spec=KafkaProducerWrapper)
    
    # Mock file content for tickers.txt
    mock_file_content = "AAPL\nMSFT\nGOOGL\nTSLA\n"
    
    with patch('src.data.inflow.inflow.yf.Tickers') as mock_tickers, \
         patch('builtins.open', mock_open(read_data=mock_file_content)):
        
        mock_tickers.return_value = Mock()
        
        ticker_reader = TickerReader(kafka_producer=mock_producer)
        
        assert isinstance(ticker_reader, TickerReader)
        assert isinstance(ticker_reader.symbols, list)
        assert len(ticker_reader.symbols) == 4
        assert ticker_reader.symbols == ['AAPL', 'MSFT', 'GOOGL', 'TSLA']
        assert ticker_reader.kafka_producer == mock_producer
        
def test_message_handler():
    """
    Tests the message_handler method of the TickerReader class.
    """
    mock_producer = Mock(spec=KafkaProducerWrapper)
    
    # Mock file content for tickers.txt
    mock_file_content = "AAPL\nMSFT\nGOOGL\nTSLA\n"
    
    with patch('src.data.inflow.inflow.yf.Tickers') as mock_tickers, \
         patch('builtins.open', mock_open(read_data=mock_file_content)):
        test_message = {
            'id': 'AAPL',
            'price': 230.9968,
            'time': '1756386138000',
            'exchange': 'NMS',
            'quote_type': 8,
            'change_percent': 0.21987511,
            'change': 0.50679016,
            'price_hint': '2'
        }
        ticker_reader = TickerReader(kafka_producer=mock_producer)

        ticker_reader.message_handler(test_message)

        # Assert that the message was sent to the Kafka topic
        mock_producer.send.assert_called_once_with(test_message)
