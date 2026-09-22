"""
REST API connector for market data ingestion.

This module provides connectors for Alpha Vantage, Yahoo Finance, IEX Cloud,
and other REST API-based market data providers with robust rate limiting,
retry logic, and error handling.
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import AsyncIterator, Dict, Any, List, Optional, Union
from dataclasses import dataclass
from enum import Enum
import logging
from urllib.parse import urljoin, urlencode

from .base_connector import BaseConnector, ConnectorConfig, ConnectorState, ConnectionError, DataError
from ..models import (
    MarketData, Quote, Trade, Bar, Symbol, DataSource, DataQuality, 
    MarketDataMetadata, AssetClass
)
from ..utils import (
    RateLimiterFactory, RateLimitConfig, RateLimitStrategy, RetryHandler,
    RetryConfig, BackoffStrategy, get_logger
)


class APIProvider(Enum):
    """Supported API providers."""
    ALPHA_VANTAGE = "alpha_vantage"
    YAHOO_FINANCE = "yahoo_finance"
    IEX_CLOUD = "iex_cloud"
    POLYGON = "polygon"
    FINNHUB = "finnhub"


@dataclass
class APIConnectorConfig(ConnectorConfig):
    """Configuration for REST API connector."""
    provider: APIProvider = APIProvider.ALPHA_VANTAGE
    base_url: str = ""
    api_key: Optional[str] = None
    requests_per_minute: int = 5
    requests_per_second: float = 0.1
    connection_timeout: float = 30.0
    read_timeout: float = 60.0
    max_concurrent_requests: int = 5
    user_agent: str = "QuantStream-Ingestion/1.0"
    headers: Dict[str, str] = None
    query_params: Dict[str, str] = None
    response_format: str = "json"
    pagination_support: bool = False
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {}
        if self.query_params is None:
            self.query_params = {}


class RestAPIConnector(BaseConnector):
    """Base REST API connector with rate limiting and retry logic."""
    
    def __init__(self, config: APIConnectorConfig):
        self.api_config = config
        super().__init__(config, self._get_data_source())
        self.session: Optional[aiohttp.ClientSession] = None
        self.rate_limiter = None
        self.retry_handler = None
        self._request_semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        
        # Setup rate limiter
        rate_limit_config = RateLimitConfig(
            requests_per_minute=config.requests_per_minute,
            requests_per_second=config.requests_per_second,
            strategy=RateLimitStrategy.TOKEN_BUCKET,
            burst_size=max(1, min(10, config.requests_per_minute // 6))  # Allow short bursts
        )
        self.rate_limiter = RateLimiterFactory.create_rate_limiter(
            rate_limit_config, 
            f"{config.provider.value}_{config.name}"
        )
        
        # Setup retry handler
        retry_config = RetryConfig(
            max_attempts=3,
            base_delay=1.0,
            max_delay=60.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_JITTER,
            retry_on_exceptions=(
                aiohttp.ClientError,
                asyncio.TimeoutError,
                ConnectionError
            )
        )
        self.retry_handler = RetryHandler(retry_config)
    
    def _get_data_source(self) -> DataSource:
        """Get data source enum based on provider."""
        provider_mapping = {
            APIProvider.ALPHA_VANTAGE: DataSource.ALPHA_VANTAGE,
            APIProvider.YAHOO_FINANCE: DataSource.YAHOO_FINANCE,
            APIProvider.IEX_CLOUD: DataSource.IEX_CLOUD,
            APIProvider.POLYGON: DataSource.POLYGON,
            APIProvider.FINNHUB: DataSource.FINNHUB
        }
        return provider_mapping.get(self.api_config.provider, DataSource.ALPHA_VANTAGE)
    
    async def _initialize(self) -> None:
        """Initialize HTTP session and validate configuration."""
        timeout = aiohttp.ClientTimeout(
            connect=self.api_config.connection_timeout,
            total=self.api_config.read_timeout
        )
        
        headers = {
            "User-Agent": self.api_config.user_agent,
            **self.api_config.headers
        }
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers=headers,
            connector=aiohttp.TCPConnector(
                limit=self.api_config.max_concurrent_requests,
                limit_per_host=self.api_config.max_concurrent_requests
            )
        )
        
        # Validate API key if required
        if self.api_config.api_key:
            await self._validate_api_key()
    
    async def _connect(self) -> None:
        """Establish connection (for REST APIs, this is a no-op)."""
        self.logger.info("REST API connector ready")
    
    async def _disconnect(self) -> None:
        """Close HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _validate_api_key(self) -> None:
        """Validate API key by making a test request."""
        try:
            # Make a simple test request based on provider
            test_url = self._build_test_url()
            async with self._request_semaphore:
                await self.rate_limiter.wait_for_availability()
                async with self.session.get(test_url) as response:
                    if response.status == 401 or response.status == 403:
                        raise ConnectionError(f"Invalid API key for {self.api_config.provider.value}")
                    elif response.status >= 400:
                        self.logger.warning(f"API test request returned {response.status}")
        except Exception as e:
            self.logger.error(f"API key validation failed: {e}")
            raise ConnectionError(f"Failed to validate API key: {e}")
    
    def _build_test_url(self) -> str:
        """Build test URL for API key validation."""
        # Override in subclasses for provider-specific test endpoints
        return self.api_config.base_url
    
    async def _fetch_data(self) -> AsyncIterator[MarketData]:
        """Fetch data from REST API endpoints."""
        for symbol in self.config.symbols:
            try:
                async for data in self._fetch_symbol_data(symbol):
                    yield data
            except Exception as e:
                self.logger.error(f"Error fetching data for {symbol}: {e}")
                await asyncio.sleep(1)  # Brief pause before continuing
    
    async def _fetch_symbol_data(self, symbol: Symbol) -> AsyncIterator[MarketData]:
        """Fetch data for a specific symbol."""
        # Override in subclasses for provider-specific implementation
        raise NotImplementedError("Subclasses must implement _fetch_symbol_data")
    
    async def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make rate-limited HTTP request with retry logic."""
        return await self.retry_handler.execute(self._make_single_request, endpoint, params)
    
    async def _make_single_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a single HTTP request."""
        url = urljoin(self.api_config.base_url, endpoint)
        
        # Combine default params with request-specific params
        query_params = {**self.api_config.query_params}
        if params:
            query_params.update(params)
        
        # Add API key if configured
        if self.api_config.api_key:
            query_params['apikey'] = self.api_config.api_key  # Common parameter name
        
        # Build full URL with query parameters
        if query_params:
            url += '?' + urlencode(query_params)
        
        async with self._request_semaphore:
            await self.rate_limiter.wait_for_availability()
            
            self.logger.debug(f"Making request to {url}")
            
            async with self.session.get(url) as response:
                if response.status == 429:  # Rate limit exceeded
                    retry_after = response.headers.get('Retry-After', '60')
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=429,
                        message=f"Rate limit exceeded, retry after {retry_after}s"
                    )
                
                if response.status >= 400:
                    error_text = await response.text()
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}: {error_text}"
                    )
                
                if self.api_config.response_format == 'json':
                    return await response.json()
                else:
                    return {"data": await response.text()}


class AlphaVantageConnector(RestAPIConnector):
    """Alpha Vantage API connector."""
    
    def __init__(self, config: APIConnectorConfig):
        if not config.base_url:
            config.base_url = "https://www.alphavantage.co/query"
        if not config.requests_per_minute:
            config.requests_per_minute = 5  # Free tier limit
        
        config.provider = APIProvider.ALPHA_VANTAGE
        super().__init__(config)
        
        # Alpha Vantage specific query parameters
        self.functions = {
            'quote': 'GLOBAL_QUOTE',
            'intraday': 'TIME_SERIES_INTRADAY',
            'daily': 'TIME_SERIES_DAILY',
        }
    
    def _build_test_url(self) -> str:
        """Build test URL for Alpha Vantage."""
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': 'AAPL',
            'apikey': self.api_config.api_key
        }
        return f"{self.api_config.base_url}?" + urlencode(params)
    
    async def _fetch_symbol_data(self, symbol: Symbol) -> AsyncIterator[MarketData]:
        """Fetch data for symbol from Alpha Vantage."""
        # Fetch quote data
        try:
            quote_data = await self._fetch_quote(symbol)
            if quote_data:
                yield quote_data
        except Exception as e:
            self.logger.error(f"Error fetching quote for {symbol}: {e}")
        
        # Fetch intraday data if configured
        if 'bar' in [dt.value for dt in self.config.data_types]:
            try:
                async for bar in self._fetch_intraday_bars(symbol):
                    yield bar
            except Exception as e:
                self.logger.error(f"Error fetching bars for {symbol}: {e}")
    
    async def _fetch_quote(self, symbol: Symbol) -> Optional[Quote]:
        """Fetch current quote from Alpha Vantage."""
        params = {
            'function': 'GLOBAL_QUOTE',
            'symbol': symbol.ticker
        }
        
        response = await self._make_request("", params)
        
        # Parse Alpha Vantage response
        if 'Global Quote' not in response:
            self.logger.warning(f"No quote data for {symbol.ticker}")
            return None
        
        quote_data = response['Global Quote']
        
        # Create metadata
        metadata = MarketDataMetadata(
            source=DataSource.ALPHA_VANTAGE,
            source_timestamp=datetime.utcnow(),
            quality=DataQuality.HIGH,
            raw_data=quote_data
        )
        
        # Parse quote fields
        try:
            return Quote(
                symbol=symbol,
                timestamp=datetime.utcnow(),  # Alpha Vantage doesn't provide real-time timestamps
                bid_price=None,  # Not available in global quote
                ask_price=None,  # Not available in global quote
                bid_size=None,
                ask_size=None,
                metadata=metadata
            )
        except Exception as e:
            self.logger.error(f"Error parsing quote data for {symbol}: {e}")
            return None
    
    async def _fetch_intraday_bars(self, symbol: Symbol) -> AsyncIterator[Bar]:
        """Fetch intraday bars from Alpha Vantage."""
        params = {
            'function': 'TIME_SERIES_INTRADAY',
            'symbol': symbol.ticker,
            'interval': '1min',
            'outputsize': 'compact'
        }
        
        response = await self._make_request("", params)
        
        # Parse time series data
        time_series_key = 'Time Series (1min)'
        if time_series_key not in response:
            self.logger.warning(f"No intraday data for {symbol.ticker}")
            return
        
        time_series = response[time_series_key]
        
        for timestamp_str, bar_data in time_series.items():
            try:
                # Parse timestamp
                timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                # Create metadata
                metadata = MarketDataMetadata(
                    source=DataSource.ALPHA_VANTAGE,
                    source_timestamp=timestamp,
                    quality=DataQuality.HIGH,
                    raw_data=bar_data
                )
                
                # Create bar
                bar = Bar(
                    symbol=symbol,
                    timestamp=timestamp,
                    timeframe="1m",
                    open_price=Decimal(bar_data['1. open']),
                    high_price=Decimal(bar_data['2. high']),
                    low_price=Decimal(bar_data['3. low']),
                    close_price=Decimal(bar_data['4. close']),
                    volume=int(bar_data['5. volume']),
                    metadata=metadata
                )
                
                yield bar
                
            except (ValueError, KeyError) as e:
                self.logger.error(f"Error parsing bar data for {symbol} at {timestamp_str}: {e}")
                continue


class YahooFinanceConnector(RestAPIConnector):
    """Yahoo Finance API connector."""
    
    def __init__(self, config: APIConnectorConfig):
        if not config.base_url:
            config.base_url = "https://query1.finance.yahoo.com"
        if not config.requests_per_minute:
            config.requests_per_minute = 60  # Generous limit
        
        config.provider = APIProvider.YAHOO_FINANCE
        super().__init__(config)
    
    def _build_test_url(self) -> str:
        """Build test URL for Yahoo Finance."""
        return f"{self.api_config.base_url}/v8/finance/chart/AAPL"
    
    async def _fetch_symbol_data(self, symbol: Symbol) -> AsyncIterator[MarketData]:
        """Fetch data for symbol from Yahoo Finance."""
        try:
            # Fetch chart data which includes quote and historical data
            async for data in self._fetch_chart_data(symbol):
                yield data
        except Exception as e:
            self.logger.error(f"Error fetching data for {symbol}: {e}")
    
    async def _fetch_chart_data(self, symbol: Symbol) -> AsyncIterator[MarketData]:
        """Fetch chart data from Yahoo Finance."""
        endpoint = f"/v8/finance/chart/{symbol.ticker}"
        params = {
            'interval': '1m',
            'range': '1d',
            'includePrePost': 'false'
        }
        
        response = await self._make_request(endpoint, params)
        
        # Parse Yahoo Finance response
        if 'chart' not in response or not response['chart']['result']:
            self.logger.warning(f"No chart data for {symbol.ticker}")
            return
        
        chart_data = response['chart']['result'][0]
        meta = chart_data.get('meta', {})
        
        # Create current quote from meta data
        if 'regularMarketPrice' in meta:
            metadata = MarketDataMetadata(
                source=DataSource.YAHOO_FINANCE,
                source_timestamp=datetime.fromtimestamp(meta.get('regularMarketTime', time.time())),
                quality=DataQuality.HIGH,
                raw_data=meta
            )
            
            quote = Quote(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(meta.get('regularMarketTime', time.time())),
                bid_price=Decimal(str(meta.get('bid', 0))) if meta.get('bid') else None,
                ask_price=Decimal(str(meta.get('ask', 0))) if meta.get('ask') else None,
                bid_size=meta.get('bidSize'),
                ask_size=meta.get('askSize'),
                metadata=metadata
            )
            yield quote
        
        # Process historical bars if available
        timestamps = chart_data.get('timestamp', [])
        indicators = chart_data.get('indicators', {})
        quote_data = indicators.get('quote', [{}])[0]
        
        if timestamps and quote_data:
            opens = quote_data.get('open', [])
            highs = quote_data.get('high', [])
            lows = quote_data.get('low', [])
            closes = quote_data.get('close', [])
            volumes = quote_data.get('volume', [])
            
            for i, ts in enumerate(timestamps):
                if (i < len(opens) and opens[i] is not None and
                    i < len(highs) and highs[i] is not None and
                    i < len(lows) and lows[i] is not None and
                    i < len(closes) and closes[i] is not None and
                    i < len(volumes) and volumes[i] is not None):
                    
                    metadata = MarketDataMetadata(
                        source=DataSource.YAHOO_FINANCE,
                        source_timestamp=datetime.fromtimestamp(ts),
                        quality=DataQuality.HIGH
                    )
                    
                    bar = Bar(
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(ts),
                        timeframe="1m",
                        open_price=Decimal(str(opens[i])),
                        high_price=Decimal(str(highs[i])),
                        low_price=Decimal(str(lows[i])),
                        close_price=Decimal(str(closes[i])),
                        volume=int(volumes[i]),
                        metadata=metadata
                    )
                    yield bar


class IEXCloudConnector(RestAPIConnector):
    """IEX Cloud API connector."""
    
    def __init__(self, config: APIConnectorConfig):
        if not config.base_url:
            config.base_url = "https://cloud.iexapis.com/stable"
        if not config.requests_per_minute:
            config.requests_per_minute = 500  # Paid tier default
        
        config.provider = APIProvider.IEX_CLOUD
        super().__init__(config)
    
    def _build_test_url(self) -> str:
        """Build test URL for IEX Cloud."""
        params = {'token': self.api_config.api_key}
        return f"{self.api_config.base_url}/stock/AAPL/quote?" + urlencode(params)
    
    async def _make_single_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Override to use token parameter instead of apikey."""
        url = urljoin(self.api_config.base_url, endpoint)
        
        # Combine default params with request-specific params
        query_params = {**self.api_config.query_params}
        if params:
            query_params.update(params)
        
        # Add token for IEX Cloud
        if self.api_config.api_key:
            query_params['token'] = self.api_config.api_key
        
        # Build full URL with query parameters
        if query_params:
            url += '?' + urlencode(query_params)
        
        async with self._request_semaphore:
            await self.rate_limiter.wait_for_availability()
            
            self.logger.debug(f"Making request to {url}")
            
            async with self.session.get(url) as response:
                if response.status >= 400:
                    error_text = await response.text()
                    raise aiohttp.ClientResponseError(
                        request_info=response.request_info,
                        history=response.history,
                        status=response.status,
                        message=f"HTTP {response.status}: {error_text}"
                    )
                
                return await response.json()
    
    async def _fetch_symbol_data(self, symbol: Symbol) -> AsyncIterator[MarketData]:
        """Fetch data for symbol from IEX Cloud."""
        try:
            # Fetch quote data
            quote = await self._fetch_quote(symbol)
            if quote:
                yield quote
        except Exception as e:
            self.logger.error(f"Error fetching quote for {symbol}: {e}")
    
    async def _fetch_quote(self, symbol: Symbol) -> Optional[Quote]:
        """Fetch current quote from IEX Cloud."""
        endpoint = f"/stock/{symbol.ticker}/quote"
        
        response = await self._make_request(endpoint)
        
        # Create metadata
        metadata = MarketDataMetadata(
            source=DataSource.IEX_CLOUD,
            source_timestamp=datetime.fromtimestamp(response.get('latestUpdate', 0) / 1000),
            quality=DataQuality.HIGH,
            raw_data=response
        )
        
        # Create quote
        try:
            return Quote(
                symbol=symbol,
                timestamp=datetime.fromtimestamp(response.get('latestUpdate', 0) / 1000),
                bid_price=Decimal(str(response['iexBidPrice'])) if response.get('iexBidPrice') else None,
                ask_price=Decimal(str(response['iexAskPrice'])) if response.get('iexAskPrice') else None,
                bid_size=response.get('iexBidSize'),
                ask_size=response.get('iexAskSize'),
                metadata=metadata
            )
        except (ValueError, KeyError) as e:
            self.logger.error(f"Error parsing quote data for {symbol}: {e}")
            return None


def create_api_connector(provider: str, config: Dict[str, Any]) -> RestAPIConnector:
    """Factory function to create API connectors."""
    api_config = APIConnectorConfig(**config)
    
    if provider.lower() == 'alpha_vantage':
        return AlphaVantageConnector(api_config)
    elif provider.lower() == 'yahoo_finance':
        return YahooFinanceConnector(api_config)
    elif provider.lower() == 'iex_cloud':
        return IEXCloudConnector(api_config)
    else:
        raise ValueError(f"Unknown API provider: {provider}")


# Example usage configurations
ALPHA_VANTAGE_CONFIG = {
    'name': 'alpha_vantage',
    'provider': 'alpha_vantage',
    'api_key': 'your_api_key',
    'symbols': ['AAPL', 'GOOGL', 'MSFT'],
    'data_types': ['quote', 'bar'],
    'requests_per_minute': 5
}

YAHOO_FINANCE_CONFIG = {
    'name': 'yahoo_finance',
    'provider': 'yahoo_finance',
    'symbols': ['AAPL', 'GOOGL', 'MSFT'],
    'data_types': ['quote', 'bar'],
    'requests_per_minute': 60
}

IEX_CLOUD_CONFIG = {
    'name': 'iex_cloud',
    'provider': 'iex_cloud',
    'api_key': 'your_iex_token',
    'symbols': ['AAPL', 'GOOGL', 'MSFT'],
    'data_types': ['quote'],
    'requests_per_minute': 500
}