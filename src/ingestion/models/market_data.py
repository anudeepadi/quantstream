"""
Market data models for the QuantStream Analytics Platform.

This module defines the core data structures for market data across different
asset classes and data sources.
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List
from uuid import uuid4


class AssetClass(Enum):
    """Asset class enumeration."""
    EQUITY = "equity"
    OPTION = "option"
    FUTURE = "future"
    CRYPTO = "crypto"
    FOREX = "forex"
    BOND = "bond"
    COMMODITY = "commodity"


class DataSource(Enum):
    """Data source enumeration."""
    ALPHA_VANTAGE = "alpha_vantage"
    YAHOO_FINANCE = "yahoo_finance"
    IEX_CLOUD = "iex_cloud"
    FINNHUB = "finnhub"
    POLYGON = "polygon"
    CSV_FILE = "csv_file"


class DataType(Enum):
    """Market data type enumeration."""
    QUOTE = "quote"
    TRADE = "trade"
    BAR = "bar"
    TICK = "tick"
    ORDER_BOOK = "order_book"
    NEWS = "news"
    FUNDAMENTAL = "fundamental"


class DataQuality(Enum):
    """Data quality indicators."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    QUESTIONABLE = "questionable"


@dataclass(frozen=True)
class Symbol:
    """Market symbol representation."""
    ticker: str
    exchange: Optional[str] = None
    asset_class: Optional[AssetClass] = None
    currency: str = "USD"
    
    def __str__(self) -> str:
        if self.exchange:
            return f"{self.ticker}:{self.exchange}"
        return self.ticker


@dataclass
class MarketDataMetadata:
    """Metadata for market data entries."""
    message_id: str = field(default_factory=lambda: str(uuid4()))
    source: Optional[DataSource] = None
    source_timestamp: Optional[datetime] = None
    ingestion_timestamp: datetime = field(default_factory=datetime.utcnow)
    quality: DataQuality = DataQuality.MEDIUM
    raw_data: Optional[Dict[str, Any]] = None
    correlation_id: Optional[str] = None
    partition_key: Optional[str] = None


@dataclass
class Quote:
    """Market quote data."""
    symbol: Symbol
    timestamp: datetime
    bid_price: Optional[Decimal]
    ask_price: Optional[Decimal]
    bid_size: Optional[int] = None
    ask_size: Optional[int] = None
    metadata: MarketDataMetadata = field(default_factory=MarketDataMetadata)
    
    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid-price if both bid and ask are available."""
        if self.bid_price is not None and self.ask_price is not None:
            return (self.bid_price + self.ask_price) / 2
        return None
    
    @property
    def spread(self) -> Optional[Decimal]:
        """Calculate bid-ask spread."""
        if self.bid_price is not None and self.ask_price is not None:
            return self.ask_price - self.bid_price
        return None


@dataclass
class Trade:
    """Market trade data."""
    symbol: Symbol
    timestamp: datetime
    price: Decimal
    size: int
    trade_id: Optional[str] = None
    conditions: Optional[List[str]] = None
    metadata: MarketDataMetadata = field(default_factory=MarketDataMetadata)


@dataclass
class Bar:
    """OHLCV bar data."""
    symbol: Symbol
    timestamp: datetime
    timeframe: str  # e.g., "1m", "5m", "1h", "1d"
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    volume: int
    vwap: Optional[Decimal] = None
    trade_count: Optional[int] = None
    metadata: MarketDataMetadata = field(default_factory=MarketDataMetadata)


@dataclass
class OrderBookLevel:
    """Order book level data."""
    price: Decimal
    size: int
    order_count: Optional[int] = None


@dataclass
class OrderBook:
    """Market order book data."""
    symbol: Symbol
    timestamp: datetime
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    sequence_number: Optional[int] = None
    metadata: MarketDataMetadata = field(default_factory=MarketDataMetadata)


@dataclass
class NewsItem:
    """Market news data."""
    symbol: Optional[Symbol]
    timestamp: datetime
    headline: str
    summary: Optional[str] = None
    source: Optional[str] = None
    url: Optional[str] = None
    sentiment: Optional[float] = None  # -1.0 to 1.0
    relevance_score: Optional[float] = None  # 0.0 to 1.0
    metadata: MarketDataMetadata = field(default_factory=MarketDataMetadata)


@dataclass
class FundamentalData:
    """Fundamental data for securities."""
    symbol: Symbol
    timestamp: datetime
    data_type: str  # e.g., "earnings", "balance_sheet", "income_statement"
    period: str  # e.g., "Q1", "annual"
    fiscal_year: int
    data: Dict[str, Any]
    metadata: MarketDataMetadata = field(default_factory=MarketDataMetadata)


# Union type for all market data types
MarketData = Quote | Trade | Bar | OrderBook | NewsItem | FundamentalData


@dataclass
class MarketDataMessage:
    """Wrapper for market data messages with routing information."""
    data_type: DataType
    data: MarketData
    topic: Optional[str] = None
    partition_key: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    
    def __post_init__(self):
        """Set default partition key and topic if not provided."""
        if self.partition_key is None:
            self.partition_key = str(self.data.symbol)
        
        if self.topic is None:
            self.topic = f"market_data_{self.data_type.value}"


@dataclass
class IngestionMetrics:
    """Metrics for ingestion performance tracking."""
    source: DataSource
    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    bytes_processed: int = 0
    processing_time_ms: float = 0.0
    last_message_timestamp: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        """Calculate processing success rate."""
        if self.messages_received == 0:
            return 0.0
        return (self.messages_processed / self.messages_received) * 100.0
    
    @property
    def throughput_per_second(self) -> float:
        """Calculate throughput in messages per second."""
        if self.processing_time_ms == 0.0:
            return 0.0
        return (self.messages_processed / self.processing_time_ms) * 1000.0