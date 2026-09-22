"""
WebSocket connector for real-time market data ingestion.

This module provides connectors for Finnhub, Polygon.io, and other WebSocket-based
market data providers with advanced connection management, reconnection logic,
and message buffering.
"""

import asyncio
import json
import time
import websockets
from collections import deque
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Dict, Any, List, Optional, Union, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
import logging
from urllib.parse import urljoin

from .base_connector import BaseConnector, ConnectorConfig, ConnectorState, ConnectionError, DataError
from ..models import (
    MarketData, Quote, Trade, Bar, Symbol, DataSource, DataQuality,
    MarketDataMetadata, AssetClass, OrderBook, OrderBookLevel
)
from ..utils import get_logger, RetryHandler, RetryConfig, BackoffStrategy


class WebSocketProvider(Enum):
    """Supported WebSocket providers."""
    FINNHUB = "finnhub"
    POLYGON = "polygon"
    ALPACA = "alpaca"
    COINBASE = "coinbase"


@dataclass
class WebSocketConnectorConfig(ConnectorConfig):
    """Configuration for WebSocket connector."""
    provider: WebSocketProvider = WebSocketProvider.FINNHUB
    ws_url: str = ""
    api_key: Optional[str] = None
    heartbeat_interval: float = 30.0
    ping_interval: float = 10.0
    reconnect_delay: float = 5.0
    max_reconnect_attempts: int = 10
    message_buffer_size: int = 10000
    connection_timeout: float = 30.0
    subscription_channels: List[str] = field(default_factory=list)
    auth_required: bool = True
    compression: bool = True
    user_agent: str = "QuantStream-Ingestion/1.0"
    
    def __post_init__(self):
        if not self.subscription_channels:
            self.subscription_channels = ["trades", "quotes"]


class CircularBuffer:
    """Thread-safe circular buffer for message buffering."""
    
    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self.buffer = deque(maxlen=maxsize)
        self._lock = asyncio.Lock()
    
    async def put(self, item: Any) -> bool:
        """Add item to buffer. Returns False if buffer is full."""
        async with self._lock:
            if len(self.buffer) >= self.maxsize:
                return False
            self.buffer.append(item)
            return True
    
    async def get(self) -> Optional[Any]:
        """Get item from buffer."""
        async with self._lock:
            if self.buffer:
                return self.buffer.popleft()
            return None
    
    async def size(self) -> int:
        """Get current buffer size."""
        async with self._lock:
            return len(self.buffer)
    
    async def clear(self):
        """Clear the buffer."""
        async with self._lock:
            self.buffer.clear()


class BaseWebSocketConnector(BaseConnector):
    """Base WebSocket connector with connection management and reconnection logic."""
    
    def __init__(self, config: WebSocketConnectorConfig):
        self.ws_config = config
        super().__init__(config, self._get_data_source())
        self.websocket = None
        self.last_ping = 0
        self.last_pong = 0
        self.reconnect_count = 0
        self.is_authenticated = False
        self.subscribed_symbols: Set[str] = set()
        
        # Message buffering
        self.message_buffer = CircularBuffer(config.message_buffer_size)
        
        # Connection management
        self._connection_lock = asyncio.Lock()
        self._ping_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        
        # Message handlers
        self._message_handlers: Dict[str, Callable] = {}
        self._setup_message_handlers()
    
    def _get_data_source(self) -> DataSource:
        """Get data source enum based on provider."""
        provider_mapping = {
            WebSocketProvider.FINNHUB: DataSource.FINNHUB,
            WebSocketProvider.POLYGON: DataSource.POLYGON,
        }
        return provider_mapping.get(self.ws_config.provider, DataSource.FINNHUB)
    
    def _setup_message_handlers(self):
        """Setup message handlers for different message types."""
        self._message_handlers = {
            'ping': self._handle_ping,
            'pong': self._handle_pong,
            'auth': self._handle_auth_response,
            'error': self._handle_error,
            'trade': self._handle_trade,
            'quote': self._handle_quote,
            'book': self._handle_order_book,
        }
    
    async def _initialize(self) -> None:
        """Initialize WebSocket connector."""
        if not self.ws_config.ws_url:
            raise ConnectionError("WebSocket URL is required")
        
        if self.ws_config.auth_required and not self.ws_config.api_key:
            raise ConnectionError("API key is required for authenticated connection")
    
    async def _connect(self) -> None:
        """Establish WebSocket connection with retry logic."""
        async with self._connection_lock:
            await self._establish_connection()
    
    async def _disconnect(self) -> None:
        """Close WebSocket connection and cleanup."""
        async with self._connection_lock:
            # Cancel background tasks
            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
            
            # Close WebSocket
            if self.websocket:
                try:
                    await self.websocket.close()
                except Exception as e:
                    self.logger.error(f"Error closing WebSocket: {e}")
                finally:
                    self.websocket = None
            
            # Reset state
            self.is_authenticated = False
            self.subscribed_symbols.clear()
            self.reconnect_count = 0
    
    async def _establish_connection(self) -> None:
        """Establish WebSocket connection."""
        try:
            self.logger.info(f"Connecting to {self.ws_config.ws_url}")
            
            # Create WebSocket connection
            extra_headers = {
                'User-Agent': self.ws_config.user_agent
            }
            
            self.websocket = await websockets.connect(
                self.ws_config.ws_url,
                extra_headers=extra_headers,
                ping_interval=self.ws_config.ping_interval,
                ping_timeout=self.ws_config.connection_timeout,
                compression="deflate" if self.ws_config.compression else None
            )
            
            self.logger.info("WebSocket connection established")
            
            # Authenticate if required
            if self.ws_config.auth_required:
                await self._authenticate()
            
            # Subscribe to symbols
            await self._subscribe_to_symbols()
            
            # Start background tasks
            self._ping_task = asyncio.create_task(self._ping_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            self.reconnect_count = 0
            
        except Exception as e:
            self.logger.error(f"Failed to establish WebSocket connection: {e}")
            raise ConnectionError(f"WebSocket connection failed: {e}")
    
    async def _authenticate(self) -> None:
        """Authenticate with the WebSocket server."""
        # Override in subclasses for provider-specific authentication
        auth_message = self._build_auth_message()
        if auth_message:
            await self._send_message(auth_message)
            
            # Wait for authentication response
            timeout = 10.0
            start_time = time.time()
            while not self.is_authenticated and (time.time() - start_time) < timeout:
                await asyncio.sleep(0.1)
            
            if not self.is_authenticated:
                raise ConnectionError("Authentication failed")
    
    def _build_auth_message(self) -> Optional[Dict[str, Any]]:
        """Build authentication message. Override in subclasses."""
        return None
    
    async def _subscribe_to_symbols(self) -> None:
        """Subscribe to configured symbols."""
        for symbol in self.config.symbols:
            for channel in self.ws_config.subscription_channels:
                subscribe_message = self._build_subscribe_message(symbol, channel)
                if subscribe_message:
                    await self._send_message(subscribe_message)
                    self.subscribed_symbols.add(f"{symbol.ticker}:{channel}")
    
    def _build_subscribe_message(self, symbol: Symbol, channel: str) -> Optional[Dict[str, Any]]:
        """Build subscription message. Override in subclasses."""
        return None
    
    async def _send_message(self, message: Dict[str, Any]) -> None:
        """Send message to WebSocket server."""
        if self.websocket:
            try:
                await self.websocket.send(json.dumps(message))
            except Exception as e:
                self.logger.error(f"Failed to send WebSocket message: {e}")
                raise
    
    async def _ping_loop(self) -> None:
        """Send periodic pings to keep connection alive."""
        while not self.is_stopped and self.websocket:
            try:
                await asyncio.sleep(self.ws_config.ping_interval)
                if self.websocket:
                    ping_message = self._build_ping_message()
                    if ping_message:
                        await self._send_message(ping_message)
                    self.last_ping = time.time()
            except Exception as e:
                self.logger.error(f"Error in ping loop: {e}")
                break
    
    def _build_ping_message(self) -> Optional[Dict[str, Any]]:
        """Build ping message. Override in subclasses."""
        return {"type": "ping", "ts": int(time.time() * 1000)}
    
    async def _heartbeat_loop(self) -> None:
        """Monitor connection health and trigger reconnection if needed."""
        while not self.is_stopped and self.websocket:
            try:
                await asyncio.sleep(self.ws_config.heartbeat_interval)
                
                # Check if we've received a pong recently
                if (self.last_ping > 0 and self.last_pong > 0 and
                    self.last_ping > self.last_pong and
                    (time.time() - self.last_ping) > self.ws_config.heartbeat_interval * 2):
                    
                    self.logger.warning("No pong received, connection may be stale")
                    await self._handle_connection_lost()
                    break
                    
            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
                break
    
    async def _handle_connection_lost(self) -> None:
        """Handle connection loss and attempt reconnection."""
        self.state = ConnectorState.RECONNECTING
        
        if self.reconnect_count < self.ws_config.max_reconnect_attempts:
            self.reconnect_count += 1
            delay = min(
                self.ws_config.reconnect_delay * (2 ** (self.reconnect_count - 1)),
                60.0  # Max delay of 1 minute
            )
            
            self.logger.info(f"Attempting reconnection {self.reconnect_count}/{self.ws_config.max_reconnect_attempts} in {delay}s")
            await asyncio.sleep(delay)
            
            try:
                await self._disconnect()
                await self._establish_connection()
                self.state = ConnectorState.CONNECTED
            except Exception as e:
                self.logger.error(f"Reconnection attempt failed: {e}")
                if self.reconnect_count >= self.ws_config.max_reconnect_attempts:
                    self.logger.error("Max reconnection attempts reached")
                    self.state = ConnectorState.ERROR
        else:
            self.logger.error("Max reconnection attempts reached")
            self.state = ConnectorState.ERROR
    
    async def _fetch_data(self) -> AsyncIterator[MarketData]:
        """Fetch data from WebSocket connection."""
        while not self.is_stopped and self.websocket:
            try:
                # Check for buffered messages first
                buffered_message = await self.message_buffer.get()
                if buffered_message:
                    yield buffered_message
                    continue
                
                # Read from WebSocket
                try:
                    message = await asyncio.wait_for(
                        self.websocket.recv(), 
                        timeout=1.0
                    )
                    await self._process_websocket_message(message)
                except asyncio.TimeoutError:
                    # No message received, continue
                    continue
                except websockets.exceptions.ConnectionClosed:
                    self.logger.warning("WebSocket connection closed")
                    await self._handle_connection_lost()
                    break
                    
            except Exception as e:
                self.logger.error(f"Error in data fetch loop: {e}")
                await asyncio.sleep(1)
    
    async def _process_websocket_message(self, raw_message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            message = json.loads(raw_message)
            
            # Determine message type
            msg_type = self._get_message_type(message)
            
            # Handle message based on type
            if msg_type in self._message_handlers:
                await self._message_handlers[msg_type](message)
            else:
                self.logger.debug(f"Unhandled message type: {msg_type}")
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {e}")
    
    def _get_message_type(self, message: Dict[str, Any]) -> str:
        """Extract message type from message. Override in subclasses."""
        return message.get('type', 'unknown')
    
    # Default message handlers
    async def _handle_ping(self, message: Dict[str, Any]) -> None:
        """Handle ping message."""
        pong_message = {"type": "pong", "ts": message.get("ts")}
        await self._send_message(pong_message)
    
    async def _handle_pong(self, message: Dict[str, Any]) -> None:
        """Handle pong message."""
        self.last_pong = time.time()
    
    async def _handle_auth_response(self, message: Dict[str, Any]) -> None:
        """Handle authentication response."""
        if message.get('status') == 'success':
            self.is_authenticated = True
            self.logger.info("WebSocket authentication successful")
        else:
            self.logger.error(f"WebSocket authentication failed: {message}")
            raise ConnectionError("Authentication failed")
    
    async def _handle_error(self, message: Dict[str, Any]) -> None:
        """Handle error message."""
        error_msg = message.get('message', 'Unknown error')
        self.logger.error(f"WebSocket error: {error_msg}")
    
    async def _handle_trade(self, message: Dict[str, Any]) -> None:
        """Handle trade message. Override in subclasses."""
        pass
    
    async def _handle_quote(self, message: Dict[str, Any]) -> None:
        """Handle quote message. Override in subclasses."""
        pass
    
    async def _handle_order_book(self, message: Dict[str, Any]) -> None:
        """Handle order book message. Override in subclasses."""
        pass


class FinnhubWebSocketConnector(BaseWebSocketConnector):
    """Finnhub WebSocket connector."""
    
    def __init__(self, config: WebSocketConnectorConfig):
        if not config.ws_url:
            config.ws_url = "wss://ws.finnhub.io"
        config.provider = WebSocketProvider.FINNHUB
        super().__init__(config)
    
    def _build_auth_message(self) -> Optional[Dict[str, Any]]:
        """Build Finnhub authentication message."""
        return {
            "type": "auth",
            "token": self.ws_config.api_key
        }
    
    def _build_subscribe_message(self, symbol: Symbol, channel: str) -> Optional[Dict[str, Any]]:
        """Build Finnhub subscription message."""
        return {
            "type": "subscribe",
            "symbol": symbol.ticker
        }
    
    def _get_message_type(self, message: Dict[str, Any]) -> str:
        """Extract message type from Finnhub message."""
        if "type" in message:
            return message["type"]
        elif "data" in message:
            return "trade"  # Finnhub trade data
        return "unknown"
    
    async def _handle_trade(self, message: Dict[str, Any]) -> None:
        """Handle Finnhub trade message."""
        if "data" not in message:
            return
        
        for trade_data in message["data"]:
            try:
                symbol = Symbol(ticker=trade_data.get("s", ""))
                
                metadata = MarketDataMetadata(
                    source=DataSource.FINNHUB,
                    source_timestamp=datetime.fromtimestamp(trade_data.get("t", 0) / 1000),
                    quality=DataQuality.HIGH,
                    raw_data=trade_data
                )
                
                trade = Trade(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(trade_data.get("t", 0) / 1000),
                    price=Decimal(str(trade_data.get("p", 0))),
                    size=int(trade_data.get("v", 0)),
                    metadata=metadata
                )
                
                # Buffer the trade for processing
                await self.message_buffer.put(trade)
                
            except Exception as e:
                self.logger.error(f"Error parsing Finnhub trade data: {e}")


class PolygonWebSocketConnector(BaseWebSocketConnector):
    """Polygon.io WebSocket connector."""
    
    def __init__(self, config: WebSocketConnectorConfig):
        if not config.ws_url:
            config.ws_url = "wss://socket.polygon.io/stocks"
        config.provider = WebSocketProvider.POLYGON
        super().__init__(config)
    
    def _build_auth_message(self) -> Optional[Dict[str, Any]]:
        """Build Polygon authentication message."""
        return {
            "action": "auth",
            "params": self.ws_config.api_key
        }
    
    def _build_subscribe_message(self, symbol: Symbol, channel: str) -> Optional[Dict[str, Any]]:
        """Build Polygon subscription message."""
        # Map channel types to Polygon message types
        channel_mapping = {
            "trades": "T",
            "quotes": "Q",
            "bars": "AM"
        }
        
        msg_type = channel_mapping.get(channel, "T")
        
        return {
            "action": "subscribe",
            "params": f"{msg_type}.{symbol.ticker}"
        }
    
    def _get_message_type(self, message: Dict[str, Any]) -> str:
        """Extract message type from Polygon message."""
        if isinstance(message, list) and message:
            first_msg = message[0]
            if isinstance(first_msg, dict):
                event_type = first_msg.get("ev", "")
                if event_type == "T":
                    return "trade"
                elif event_type == "Q":
                    return "quote"
                elif event_type == "AM":
                    return "bar"
        
        if isinstance(message, dict):
            if message.get("status") == "auth_success":
                return "auth"
            elif "error" in message:
                return "error"
        
        return "unknown"
    
    async def _handle_auth_response(self, message: Dict[str, Any]) -> None:
        """Handle Polygon authentication response."""
        if message.get("status") == "auth_success":
            self.is_authenticated = True
            self.logger.info("Polygon WebSocket authentication successful")
        else:
            self.logger.error(f"Polygon authentication failed: {message}")
            raise ConnectionError("Polygon authentication failed")
    
    async def _handle_trade(self, message: List[Dict[str, Any]]) -> None:
        """Handle Polygon trade messages."""
        if not isinstance(message, list):
            return
        
        for trade_data in message:
            if trade_data.get("ev") != "T":
                continue
            
            try:
                symbol = Symbol(ticker=trade_data.get("sym", ""))
                
                metadata = MarketDataMetadata(
                    source=DataSource.POLYGON,
                    source_timestamp=datetime.fromtimestamp(trade_data.get("t", 0) / 1000),
                    quality=DataQuality.HIGH,
                    raw_data=trade_data
                )
                
                trade = Trade(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(trade_data.get("t", 0) / 1000),
                    price=Decimal(str(trade_data.get("p", 0))),
                    size=int(trade_data.get("s", 0)),
                    trade_id=str(trade_data.get("i", "")),
                    conditions=trade_data.get("c", []),
                    metadata=metadata
                )
                
                # Buffer the trade for processing
                await self.message_buffer.put(trade)
                
            except Exception as e:
                self.logger.error(f"Error parsing Polygon trade data: {e}")
    
    async def _handle_quote(self, message: List[Dict[str, Any]]) -> None:
        """Handle Polygon quote messages."""
        if not isinstance(message, list):
            return
        
        for quote_data in message:
            if quote_data.get("ev") != "Q":
                continue
            
            try:
                symbol = Symbol(ticker=quote_data.get("sym", ""))
                
                metadata = MarketDataMetadata(
                    source=DataSource.POLYGON,
                    source_timestamp=datetime.fromtimestamp(quote_data.get("t", 0) / 1000),
                    quality=DataQuality.HIGH,
                    raw_data=quote_data
                )
                
                quote = Quote(
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(quote_data.get("t", 0) / 1000),
                    bid_price=Decimal(str(quote_data.get("bp", 0))) if quote_data.get("bp") else None,
                    ask_price=Decimal(str(quote_data.get("ap", 0))) if quote_data.get("ap") else None,
                    bid_size=quote_data.get("bs"),
                    ask_size=quote_data.get("as"),
                    metadata=metadata
                )
                
                # Buffer the quote for processing
                await self.message_buffer.put(quote)
                
            except Exception as e:
                self.logger.error(f"Error parsing Polygon quote data: {e}")


def create_websocket_connector(provider: str, config: Dict[str, Any]) -> BaseWebSocketConnector:
    """Factory function to create WebSocket connectors."""
    ws_config = WebSocketConnectorConfig(**config)
    
    if provider.lower() == 'finnhub':
        return FinnhubWebSocketConnector(ws_config)
    elif provider.lower() == 'polygon':
        return PolygonWebSocketConnector(ws_config)
    else:
        raise ValueError(f"Unknown WebSocket provider: {provider}")


# Example usage configurations
FINNHUB_WEBSOCKET_CONFIG = {
    'name': 'finnhub_websocket',
    'provider': 'finnhub',
    'api_key': 'your_finnhub_token',
    'symbols': ['AAPL', 'GOOGL', 'MSFT'],
    'subscription_channels': ['trades'],
    'message_buffer_size': 10000
}

POLYGON_WEBSOCKET_CONFIG = {
    'name': 'polygon_websocket',
    'provider': 'polygon',
    'api_key': 'your_polygon_token',
    'symbols': ['AAPL', 'GOOGL', 'MSFT'],
    'subscription_channels': ['trades', 'quotes'],
    'message_buffer_size': 20000
}