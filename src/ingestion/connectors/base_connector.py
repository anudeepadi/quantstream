"""
Base connector framework for market data ingestion.

This module provides the abstract base class and common functionality
for all data source connectors.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncIterator, Dict, Any, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import signal
import threading

from ..models import (
    MarketData, MarketDataMessage, DataSource, DataType, 
    IngestionMetrics, Symbol, AssetClass
)


class ConnectorState(Enum):
    """Connector lifecycle states."""
    INITIALIZING = "initializing"
    READY = "ready"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DISCONNECTING = "disconnecting"
    DISCONNECTED = "disconnected"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass
class ConnectorConfig:
    """Base configuration for all connectors."""
    name: str
    enabled: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0
    batch_size: int = 100
    max_queue_size: int = 10000
    metrics_interval: float = 60.0
    health_check_interval: float = 30.0
    symbols: List[Symbol] = field(default_factory=list)
    data_types: List[DataType] = field(default_factory=list)
    custom_config: Dict[str, Any] = field(default_factory=dict)


class ConnectorError(Exception):
    """Base exception for connector errors."""
    pass


class ConfigurationError(ConnectorError):
    """Configuration related errors."""
    pass


class ConnectionError(ConnectorError):
    """Connection related errors."""
    pass


class DataError(ConnectorError):
    """Data processing related errors."""
    pass


class BaseConnector(ABC):
    """
    Abstract base class for all market data connectors.
    
    Provides common functionality for connection management, error handling,
    metrics collection, and data processing pipeline integration.
    """
    
    def __init__(self, config: ConnectorConfig, data_source: DataSource):
        self.config = config
        self.data_source = data_source
        self.state = ConnectorState.INITIALIZING
        self.logger = logging.getLogger(f"{self.__class__.__name__}.{config.name}")
        
        # Internal state
        self._stop_event = threading.Event()
        self._message_queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._metrics = IngestionMetrics(source=data_source)
        self._last_health_check = datetime.utcnow()
        self._error_count = 0
        self._consecutive_errors = 0
        self._subscribers: Set[Callable[[MarketDataMessage], None]] = set()
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"{config.name}-worker")
        
        # Async tasks
        self._tasks: List[asyncio.Task] = []
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info(f"Initialized {self.__class__.__name__} connector: {config.name}")
    
    @property
    def is_connected(self) -> bool:
        """Check if connector is in a connected state."""
        return self.state == ConnectorState.CONNECTED
    
    @property
    def is_stopped(self) -> bool:
        """Check if connector is stopped."""
        return self._stop_event.is_set() or self.state == ConnectorState.STOPPED
    
    @property
    def metrics(self) -> IngestionMetrics:
        """Get current metrics snapshot."""
        return self._metrics
    
    def subscribe(self, callback: Callable[[MarketDataMessage], None]) -> None:
        """Subscribe to market data messages."""
        self._subscribers.add(callback)
        self.logger.debug(f"Added subscriber: {callback.__name__}")
    
    def unsubscribe(self, callback: Callable[[MarketDataMessage], None]) -> None:
        """Unsubscribe from market data messages."""
        self._subscribers.discard(callback)
        self.logger.debug(f"Removed subscriber: {callback.__name__}")
    
    async def start(self) -> None:
        """Start the connector and begin data ingestion."""
        if self.state != ConnectorState.INITIALIZING:
            raise ConnectorError(f"Cannot start connector in state: {self.state}")
        
        try:
            self.state = ConnectorState.READY
            self.logger.info("Starting connector...")
            
            # Validate configuration
            await self._validate_config()
            
            # Initialize connection
            await self._initialize()
            
            # Start background tasks
            await self._start_background_tasks()
            
            # Connect to data source
            await self._connect()
            
            self.state = ConnectorState.CONNECTED
            self.logger.info("Connector started successfully")
            
        except Exception as e:
            self.state = ConnectorState.ERROR
            self.logger.error(f"Failed to start connector: {e}")
            await self._cleanup()
            raise
    
    async def stop(self) -> None:
        """Stop the connector gracefully."""
        if self.state == ConnectorState.STOPPED:
            return
        
        self.logger.info("Stopping connector...")
        self.state = ConnectorState.DISCONNECTING
        self._stop_event.set()
        
        try:
            # Cancel all background tasks
            for task in self._tasks:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            
            # Disconnect from data source
            await self._disconnect()
            
            # Clean up resources
            await self._cleanup()
            
            self.state = ConnectorState.STOPPED
            self.logger.info("Connector stopped")
            
        except Exception as e:
            self.logger.error(f"Error during connector shutdown: {e}")
            self.state = ConnectorState.ERROR
            raise
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check and return status information."""
        now = datetime.utcnow()
        time_since_last_message = None
        if self._metrics.last_message_timestamp:
            time_since_last_message = (now - self._metrics.last_message_timestamp).total_seconds()
        
        return {
            "connector_name": self.config.name,
            "state": self.state.value,
            "is_connected": self.is_connected,
            "uptime_seconds": (now - self._last_health_check).total_seconds(),
            "messages_processed": self._metrics.messages_processed,
            "messages_failed": self._metrics.messages_failed,
            "success_rate": self._metrics.success_rate,
            "throughput_per_second": self._metrics.throughput_per_second,
            "error_count": self._error_count,
            "consecutive_errors": self._consecutive_errors,
            "queue_size": self._message_queue.qsize(),
            "time_since_last_message_seconds": time_since_last_message,
            "last_error": self._metrics.errors[-1] if self._metrics.errors else None
        }
    
    # Abstract methods that must be implemented by subclasses
    
    @abstractmethod
    async def _initialize(self) -> None:
        """Initialize the connector. Called once during startup."""
        pass
    
    @abstractmethod
    async def _connect(self) -> None:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    async def _disconnect(self) -> None:
        """Disconnect from the data source."""
        pass
    
    @abstractmethod
    async def _fetch_data(self) -> AsyncIterator[MarketData]:
        """Fetch data from the source. Main data ingestion loop."""
        pass
    
    # Protected methods for subclass use
    
    async def _validate_config(self) -> None:
        """Validate connector configuration. Override in subclasses."""
        if not self.config.name:
            raise ConfigurationError("Connector name is required")
        
        if self.config.max_retries < 0:
            raise ConfigurationError("max_retries must be non-negative")
        
        if self.config.timeout <= 0:
            raise ConfigurationError("timeout must be positive")
    
    async def _start_background_tasks(self) -> None:
        """Start background tasks for the connector."""
        # Message processing task
        self._tasks.append(asyncio.create_task(self._message_processor()))
        
        # Health check task
        self._tasks.append(asyncio.create_task(self._health_check_loop()))
        
        # Metrics collection task
        self._tasks.append(asyncio.create_task(self._metrics_collection_loop()))
        
        # Data ingestion task
        self._tasks.append(asyncio.create_task(self._data_ingestion_loop()))
    
    async def _data_ingestion_loop(self) -> None:
        """Main data ingestion loop."""
        while not self.is_stopped:
            try:
                if not self.is_connected:
                    await asyncio.sleep(1)
                    continue
                
                received_data = False
                async for market_data in self._fetch_data():
                    received_data = True
                    if self.is_stopped:
                        break
                    
                    # Create message wrapper
                    message = MarketDataMessage(
                        data_type=self._determine_data_type(market_data),
                        data=market_data
                    )
                    
                    # Add to processing queue
                    try:
                        await asyncio.wait_for(
                            self._message_queue.put(message),
                            timeout=1.0
                        )
                        self._metrics.messages_received += 1
                    except asyncio.TimeoutError:
                        self.logger.warning("Message queue is full, dropping message")
                        self._metrics.messages_failed += 1
                        continue

                # Empty or finite sources must yield so stop/health tasks can run.
                await asyncio.sleep(0 if received_data else self.config.retry_delay)

            except Exception as e:
                self._handle_error("Data ingestion error", e)
                await asyncio.sleep(self.config.retry_delay)
    
    async def _message_processor(self) -> None:
        """Process messages from the queue and notify subscribers."""
        while not self.is_stopped:
            try:
                # Get message from queue
                try:
                    message = await asyncio.wait_for(
                        self._message_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue
                
                # Process message
                await self._process_message(message)
                
                # Notify subscribers
                for subscriber in self._subscribers.copy():
                    try:
                        await asyncio.get_event_loop().run_in_executor(
                            self._executor,
                            subscriber,
                            message
                        )
                    except Exception as e:
                        self.logger.error(f"Error in subscriber {subscriber.__name__}: {e}")
                
                self._metrics.messages_processed += 1
                self._metrics.last_message_timestamp = datetime.utcnow()
                self._consecutive_errors = 0
                
            except Exception as e:
                self._handle_error("Message processing error", e)
                self._metrics.messages_failed += 1
    
    async def _process_message(self, message: MarketDataMessage) -> None:
        """Process individual message. Override in subclasses for custom processing."""
        # Default implementation - just pass through
        pass
    
    async def _health_check_loop(self) -> None:
        """Periodic health check loop."""
        while not self.is_stopped:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                
                if self.is_stopped:
                    break
                
                # Perform health check
                health_status = await self.health_check()
                
                # Log health status periodically
                if health_status["consecutive_errors"] > 5:
                    self.logger.warning(f"Health check: {health_status}")
                
                self._last_health_check = datetime.utcnow()
                
            except Exception as e:
                self.logger.error(f"Health check error: {e}")
    
    async def _metrics_collection_loop(self) -> None:
        """Periodic metrics collection and reporting."""
        while not self.is_stopped:
            try:
                await asyncio.sleep(self.config.metrics_interval)
                
                if self.is_stopped:
                    break
                
                # Update metrics
                self._update_metrics()
                
                # Log metrics
                self.logger.info(
                    f"Metrics - Processed: {self._metrics.messages_processed}, "
                    f"Failed: {self._metrics.messages_failed}, "
                    f"Success Rate: {self._metrics.success_rate:.1f}%, "
                    f"Throughput: {self._metrics.throughput_per_second:.1f} msg/s"
                )
                
            except Exception as e:
                self.logger.error(f"Metrics collection error: {e}")
    
    def _update_metrics(self) -> None:
        """Update internal metrics. Override in subclasses for custom metrics."""
        # Calculate processing time
        if self._metrics.last_message_timestamp:
            now = datetime.utcnow()
            self._metrics.processing_time_ms = (
                now - self._metrics.last_message_timestamp
            ).total_seconds() * 1000
    
    def _determine_data_type(self, market_data: MarketData) -> DataType:
        """Determine data type from market data object."""
        type_mapping = {
            'Quote': DataType.QUOTE,
            'Trade': DataType.TRADE,
            'Bar': DataType.BAR,
            'OrderBook': DataType.ORDER_BOOK,
            'NewsItem': DataType.NEWS,
            'FundamentalData': DataType.FUNDAMENTAL
        }
        return type_mapping.get(type(market_data).__name__, DataType.QUOTE)
    
    def _handle_error(self, context: str, error: Exception) -> None:
        """Handle and log errors with context."""
        self._error_count += 1
        self._consecutive_errors += 1
        error_msg = f"{context}: {str(error)}"
        self._metrics.errors.append(error_msg)
        
        # Limit error history size
        if len(self._metrics.errors) > 100:
            self._metrics.errors = self._metrics.errors[-50:]
        
        self.logger.error(error_msg)
        
        # Transition to error state if too many consecutive errors
        if self._consecutive_errors >= 10:
            self.logger.critical("Too many consecutive errors, transitioning to error state")
            self.state = ConnectorState.ERROR
    
    async def _cleanup(self) -> None:
        """Clean up resources. Override in subclasses."""
        if self._executor:
            self._executor.shutdown(wait=True)
        
        # Clear message queue
        while not self._message_queue.empty():
            try:
                self._message_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown")
            self._stop_event.set()
        
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)


class ConnectorManager:
    """Manager for multiple connectors."""
    
    def __init__(self):
        self.connectors: Dict[str, BaseConnector] = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def register_connector(self, connector: BaseConnector) -> None:
        """Register a connector with the manager."""
        self.connectors[connector.config.name] = connector
        self.logger.info(f"Registered connector: {connector.config.name}")
    
    def unregister_connector(self, name: str) -> None:
        """Unregister a connector from the manager."""
        if name in self.connectors:
            del self.connectors[name]
            self.logger.info(f"Unregistered connector: {name}")
    
    async def start_all(self) -> None:
        """Start all registered connectors."""
        tasks = []
        for name, connector in self.connectors.items():
            if connector.config.enabled:
                tasks.append(asyncio.create_task(connector.start()))
            else:
                self.logger.info(f"Skipping disabled connector: {name}")
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    connector_name = list(self.connectors.keys())[i]
                    self.logger.error(f"Failed to start connector {connector_name}: {result}")
    
    async def stop_all(self) -> None:
        """Stop all registered connectors."""
        tasks = []
        for connector in self.connectors.values():
            if connector.state not in (ConnectorState.STOPPED, ConnectorState.DISCONNECTED):
                tasks.append(asyncio.create_task(connector.stop()))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def health_check_all(self) -> Dict[str, Dict[str, Any]]:
        """Perform health check on all connectors."""
        health_reports = {}
        for name, connector in self.connectors.items():
            try:
                health_reports[name] = await connector.health_check()
            except Exception as e:
                health_reports[name] = {
                    "error": str(e),
                    "state": "error"
                }
        return health_reports
    
    def get_connector(self, name: str) -> Optional[BaseConnector]:
        """Get connector by name."""
        return self.connectors.get(name)
    
    def list_connectors(self) -> List[str]:
        """List all registered connector names."""
        return list(self.connectors.keys())