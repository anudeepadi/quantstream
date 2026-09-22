"""
Unit tests for market data connectors.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timezone
from decimal import Decimal
import json

from src.ingestion.connectors import (
    BaseConnector, ConnectorConfig, ConnectorState, ConnectorManager,
    RestAPIConnector, AlphaVantageConnector, YahooFinanceConnector,
    APIConnectorConfig, APIProvider,
    BaseWebSocketConnector, WebSocketConnectorConfig, WebSocketProvider,
    CircularBuffer, CSVFileConnector, CSVConnectorConfig, CSVSchema
)
from src.ingestion.models import (
    Symbol, Quote, Trade, Bar, DataSource, MarketDataMetadata
)


class MockConnector(BaseConnector):
    """Mock connector for testing base functionality."""
    
    def __init__(self, config):
        super().__init__(config, DataSource.CSV_FILE)
        self.initialized = False
        self.connected = False
        self.data_items = []
    
    async def _initialize(self):
        self.initialized = True
    
    async def _connect(self):
        self.connected = True
    
    async def _disconnect(self):
        self.connected = False
    
    async def _fetch_data(self):
        for item in self.data_items:
            yield item
            await asyncio.sleep(0.01)


class TestBaseConnector:
    """Test BaseConnector functionality."""
    
    def test_connector_initialization(self):
        """Test connector initialization."""
        config = ConnectorConfig(name="test-connector")
        connector = MockConnector(config)
        
        assert connector.config == config
        assert connector.state == ConnectorState.INITIALIZING
        assert not connector.is_connected
        assert not connector.is_stopped
    
    @pytest.mark.asyncio
    async def test_connector_lifecycle(self):
        """Test connector start/stop lifecycle."""
        config = ConnectorConfig(name="test-connector")
        connector = MockConnector(config)
        
        # Start connector
        await connector.start()
        assert connector.initialized
        assert connector.connected
        assert connector.state == ConnectorState.CONNECTED
        assert connector.is_connected
        
        # Stop connector
        await connector.stop()
        assert not connector.connected
        assert connector.state == ConnectorState.STOPPED
    
    @pytest.mark.asyncio
    async def test_connector_subscribers(self):
        """Test connector message subscription."""
        config = ConnectorConfig(name="test-connector")
        connector = MockConnector(config)
        
        messages_received = []
        
        def message_handler(message):
            messages_received.append(message)
        
        # Subscribe to messages
        connector.subscribe(message_handler)
        
        # Add test data
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        connector.data_items.append(quote)
        
        # Start connector and let it process
        await connector.start()
        await asyncio.sleep(0.1)  # Let it process
        await connector.stop()
        
        # Check messages received
        assert len(messages_received) > 0
    
    @pytest.mark.asyncio
    async def test_connector_health_check(self):
        """Test connector health check."""
        config = ConnectorConfig(name="test-connector")
        connector = MockConnector(config)
        
        await connector.start()
        
        health_status = await connector.health_check()
        assert isinstance(health_status, dict)
        assert "connector_name" in health_status
        assert "state" in health_status
        assert "is_connected" in health_status
        assert health_status["connector_name"] == "test-connector"
        assert health_status["is_connected"] is True
        
        await connector.stop()


class TestConnectorManager:
    """Test ConnectorManager functionality."""
    
    @pytest.mark.asyncio
    async def test_connector_registration(self):
        """Test connector registration and management."""
        manager = ConnectorManager()
        
        # Register connectors
        config1 = ConnectorConfig(name="connector-1")
        connector1 = MockConnector(config1)
        
        config2 = ConnectorConfig(name="connector-2")
        connector2 = MockConnector(config2)
        
        manager.register_connector(connector1)
        manager.register_connector(connector2)
        
        assert len(manager.connectors) == 2
        assert "connector-1" in manager.connectors
        assert "connector-2" in manager.connectors
        
        # Test retrieval
        retrieved = manager.get_connector("connector-1")
        assert retrieved == connector1
        
        # Test listing
        names = manager.list_connectors()
        assert "connector-1" in names
        assert "connector-2" in names
    
    @pytest.mark.asyncio
    async def test_connector_manager_start_stop(self):
        """Test starting and stopping all connectors."""
        manager = ConnectorManager()
        
        # Register connectors
        config1 = ConnectorConfig(name="connector-1", enabled=True)
        connector1 = MockConnector(config1)
        
        config2 = ConnectorConfig(name="connector-2", enabled=False)
        connector2 = MockConnector(config2)
        
        manager.register_connector(connector1)
        manager.register_connector(connector2)
        
        # Start all connectors
        await manager.start_all()
        
        # Only enabled connector should be started
        assert connector1.is_connected
        assert not connector2.is_connected
        
        # Stop all connectors
        await manager.stop_all()
        
        assert not connector1.is_connected
        assert not connector2.is_connected
    
    @pytest.mark.asyncio
    async def test_connector_manager_health_check(self):
        """Test health check for all connectors."""
        manager = ConnectorManager()
        
        config1 = ConnectorConfig(name="connector-1")
        connector1 = MockConnector(config1)
        manager.register_connector(connector1)
        
        await connector1.start()
        
        health_reports = await manager.health_check_all()
        assert "connector-1" in health_reports
        assert health_reports["connector-1"]["is_connected"] is True
        
        await connector1.stop()


class TestCircularBuffer:
    """Test CircularBuffer functionality."""
    
    @pytest.mark.asyncio
    async def test_buffer_operations(self):
        """Test basic buffer operations."""
        buffer = CircularBuffer(maxsize=3)
        
        # Add items
        assert await buffer.put("item1") is True
        assert await buffer.put("item2") is True
        assert await buffer.put("item3") is True
        
        # Buffer should be at capacity
        assert await buffer.size() == 3
        
        # Adding another item should fail
        assert await buffer.put("item4") is False
        
        # Retrieve items
        item1 = await buffer.get()
        assert item1 == "item1"
        
        item2 = await buffer.get()
        assert item2 == "item2"
        
        # Now we can add another item
        assert await buffer.put("item4") is True
        
        # Check remaining items
        assert await buffer.size() == 2
    
    @pytest.mark.asyncio
    async def test_buffer_empty(self):
        """Test empty buffer behavior."""
        buffer = CircularBuffer(maxsize=3)
        
        item = await buffer.get()
        assert item is None
        
        assert await buffer.size() == 0
    
    @pytest.mark.asyncio
    async def test_buffer_clear(self):
        """Test buffer clearing."""
        buffer = CircularBuffer(maxsize=3)
        
        await buffer.put("item1")
        await buffer.put("item2")
        
        assert await buffer.size() == 2
        
        await buffer.clear()
        assert await buffer.size() == 0


class TestRestAPIConnector:
    """Test REST API connector functionality."""
    
    def test_api_connector_config(self):
        """Test API connector configuration."""
        config = APIConnectorConfig(
            name="alpha-vantage",
            provider=APIProvider.ALPHA_VANTAGE,
            api_key="test-key",
            requests_per_minute=5
        )
        
        assert config.name == "alpha-vantage"
        assert config.provider == APIProvider.ALPHA_VANTAGE
        assert config.api_key == "test-key"
        assert config.requests_per_minute == 5
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_rest_connector_initialization(self, mock_session):
        """Test REST connector initialization."""
        config = APIConnectorConfig(
            name="test-api",
            provider=APIProvider.ALPHA_VANTAGE,
            base_url="https://api.example.com",
            api_key="test-key"
        )
        
        connector = RestAPIConnector(config)
        
        # Mock session creation
        mock_session_instance = AsyncMock()
        mock_session.return_value = mock_session_instance
        response = AsyncMock()
        response.status = 200
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=None)
        mock_session_instance.get = Mock(return_value=context)
        
        await connector._initialize()
        
        assert connector.session is not None
        mock_session.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_alpha_vantage_connector(self, mock_session):
        """Test Alpha Vantage specific connector."""
        config = APIConnectorConfig(
            name="alpha-vantage",
            api_key="test-key"
        )
        
        connector = AlphaVantageConnector(config)
        
        # Check that base URL is set correctly
        assert "alphavantage.co" in connector.api_config.base_url
        assert connector.api_config.provider == APIProvider.ALPHA_VANTAGE


class TestWebSocketConnector:
    """Test WebSocket connector functionality."""
    
    def test_websocket_config(self):
        """Test WebSocket connector configuration."""
        config = WebSocketConnectorConfig(
            name="finnhub-ws",
            provider=WebSocketProvider.FINNHUB,
            ws_url="wss://ws.finnhub.io",
            api_key="test-key"
        )
        
        assert config.name == "finnhub-ws"
        assert config.provider == WebSocketProvider.FINNHUB
        assert config.ws_url == "wss://ws.finnhub.io"
        assert config.api_key == "test-key"
    
    @pytest.mark.asyncio
    async def test_websocket_message_buffering(self):
        """Test WebSocket message buffering."""
        config = WebSocketConnectorConfig(
            name="test-ws",
            message_buffer_size=100
        )
        
        connector = BaseWebSocketConnector(config)
        
        # Test buffer initialization
        assert connector.message_buffer is not None
        assert connector.message_buffer.maxsize == 100


class TestCSVConnector:
    """Test CSV file connector functionality."""
    
    def test_csv_config(self):
        """Test CSV connector configuration."""
        schema = CSVSchema(
            columns={"timestamp": "datetime", "symbol": "string", "price": "decimal"},
            required_columns=["timestamp", "symbol", "price"]
        )
        
        config = CSVConnectorConfig(
            name="csv-historical",
            file_path="/path/to/data.csv",
            schema=schema,
            batch_size=1000
        )
        
        assert config.name == "csv-historical"
        assert config.file_path == "/path/to/data.csv"
        assert config.schema == schema
        assert config.batch_size == 1000
    
    def test_csv_schema_validation(self):
        """Test CSV schema validation."""
        # Valid schema
        valid_schema = CSVSchema(
            columns={"timestamp": "datetime", "symbol": "string"},
            required_columns=["timestamp", "symbol"]
        )
        assert valid_schema.validate() is True
        
        # Invalid schema (required column not in columns)
        invalid_schema = CSVSchema(
            columns={"timestamp": "datetime"},
            required_columns=["timestamp", "symbol"]  # symbol not in columns
        )
        assert invalid_schema.validate() is False
        
        # Empty schema
        empty_schema = CSVSchema(columns={})
        assert empty_schema.validate() is False
    
    @pytest.mark.asyncio
    async def test_csv_connector_initialization(self):
        """Test CSV connector initialization."""
        schema = CSVSchema(
            columns={"timestamp": "datetime", "symbol": "string", "price": "decimal"},
            required_columns=["timestamp", "symbol", "price"]
        )
        
        config = CSVConnectorConfig(
            name="csv-test",
            file_path="/nonexistent/path.csv",  # Will fail validation
            schema=schema,
            auto_detect_schema=False
        )
        
        connector = CSVFileConnector(config)
        
        # Should raise error for non-existent file
        with pytest.raises(Exception):  # DataError specifically
            await connector._initialize()


class TestConnectorIntegration:
    """Test connector integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_multiple_connectors_with_manager(self):
        """Test running multiple connectors through manager."""
        manager = ConnectorManager()
        
        # Create multiple mock connectors
        configs = [
            ConnectorConfig(name=f"connector-{i}", enabled=True)
            for i in range(3)
        ]
        
        connectors = [MockConnector(config) for config in configs]
        
        # Register all connectors
        for connector in connectors:
            manager.register_connector(connector)
        
        assert len(manager.connectors) == 3
        
        # Start all
        await manager.start_all()
        
        # Verify all are connected
        for connector in connectors:
            assert connector.is_connected
        
        # Health check
        health_reports = await manager.health_check_all()
        assert len(health_reports) == 3
        
        for report in health_reports.values():
            assert report["is_connected"] is True
        
        # Stop all
        await manager.stop_all()
        
        # Verify all are disconnected
        for connector in connectors:
            assert not connector.is_connected


if __name__ == "__main__":
    pytest.main([__file__])