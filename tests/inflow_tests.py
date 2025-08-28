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
        

    

