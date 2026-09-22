"""
Data processor for transforming and validating market data.

This module provides comprehensive data processing capabilities including
validation, transformation, enrichment, and normalization of market data
from various sources.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Dict, Any, List, Optional, Callable, Union, AsyncIterator
from dataclasses import dataclass, field, replace
from enum import Enum
import logging
from collections import deque, defaultdict
import time

from ..models import (
    MarketData, MarketDataMessage, Quote, Trade, Bar, Symbol,
    DataSource, DataQuality, MarketDataMetadata, DataType
)
from ..models.validation import DataQualityChecker, ValidationReport
from ..utils import get_logger, MetricRegistry, timer, counter, gauge


class ProcessingStage(Enum):
    """Data processing stages."""
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    ENRICHMENT = "enrichment"
    NORMALIZATION = "normalization"
    AGGREGATION = "aggregation"
    OUTPUT = "output"


class ProcessingAction(Enum):
    """Actions to take on processed data."""
    PASS = "pass"
    MODIFY = "modify"
    DROP = "drop"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


@dataclass
class ProcessingResult:
    """Result of data processing operation."""
    action: ProcessingAction
    data: Optional[MarketData] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    processing_time_ms: float = 0.0
    stage: Optional[ProcessingStage] = None


@dataclass
class ProcessorConfig:
    """Configuration for data processors."""
    enabled: bool = True
    priority: int = 100
    parallel_processing: bool = True
    max_workers: int = 4
    batch_size: int = 100
    timeout_ms: float = 5000.0
    retry_attempts: int = 3
    drop_invalid_data: bool = False
    enable_metrics: bool = True
    custom_config: Dict[str, Any] = field(default_factory=dict)


class BaseProcessor(ABC):
    """Abstract base class for data processors."""
    
    def __init__(self, config: ProcessorConfig, name: str):
        self.config = config
        self.name = name
        self.logger = get_logger(f"{self.__class__.__name__}.{name}")
        self.metrics_registry = MetricRegistry(f"processor_{name}")
        
        # Metrics
        if config.enable_metrics:
            self._setup_metrics()
    
    def _setup_metrics(self):
        """Setup processor-specific metrics."""
        self.processed_counter = self.metrics_registry.counter(
            "messages_processed_total",
            "Total number of messages processed"
        )
        self.error_counter = self.metrics_registry.counter(
            "messages_failed_total",
            "Total number of messages that failed processing"
        )
        self.processing_timer = self.metrics_registry.timer(
            "processing_duration",
            "Time spent processing messages"
        )
        self.batch_size_gauge = self.metrics_registry.gauge(
            "current_batch_size",
            "Current batch size being processed"
        )
    
    @abstractmethod
    async def process(self, data: MarketData) -> ProcessingResult:
        """Process a single market data item."""
        pass
    
    async def process_batch(self, data_batch: List[MarketData]) -> List[ProcessingResult]:
        """Process a batch of market data items."""
        if self.config.enable_metrics:
            self.batch_size_gauge.record(len(data_batch))
        
        if self.config.parallel_processing:
            return await self._process_batch_parallel(data_batch)
        else:
            return await self._process_batch_sequential(data_batch)
    
    async def _process_batch_parallel(self, data_batch: List[MarketData]) -> List[ProcessingResult]:
        """Process batch in parallel."""
        semaphore = asyncio.Semaphore(self.config.max_workers)
        tasks = []
        
        for data in data_batch:
            task = asyncio.create_task(self._process_with_semaphore(semaphore, data))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Error processing item {i}: {result}")
                if self.config.enable_metrics:
                    self.error_counter.record()
                processed_results.append(ProcessingResult(
                    action=ProcessingAction.DROP,
                    errors=[str(result)]
                ))
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _process_batch_sequential(self, data_batch: List[MarketData]) -> List[ProcessingResult]:
        """Process batch sequentially."""
        results = []
        for data in data_batch:
            try:
                result = await self.process(data)
                results.append(result)
            except Exception as e:
                self.logger.error(f"Error processing data: {e}")
                if self.config.enable_metrics:
                    self.error_counter.record()
                results.append(ProcessingResult(
                    action=ProcessingAction.DROP,
                    errors=[str(e)]
                ))
        return results
    
    async def _process_with_semaphore(self, semaphore: asyncio.Semaphore, 
                                    data: MarketData) -> ProcessingResult:
        """Process data with semaphore for concurrency control."""
        async with semaphore:
            return await self.process(data)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics."""
        if not self.config.enable_metrics:
            return {}
        
        return {
            "messages_processed": self.processed_counter.get_value(),
            "messages_failed": self.error_counter.get_value(),
            "processing_stats": self.processing_timer.get_value(),
            "current_batch_size": self.batch_size_gauge.get_value()
        }


class ValidationProcessor(BaseProcessor):
    """Data validation processor."""
    
    def __init__(self, config: ProcessorConfig):
        super().__init__(config, "validator")
        self.quality_checker = DataQualityChecker(strict_mode=False)
        self.min_quality_threshold = DataQuality.MEDIUM
    
    async def process(self, data: MarketData) -> ProcessingResult:
        """Validate market data."""
        start_time = time.time()
        
        try:
            # Perform validation
            validation_report = self.quality_checker.check_data_quality(data)
            
            # Check quality threshold
            if validation_report.data_quality.value < self.min_quality_threshold.value:
                if self.config.drop_invalid_data:
                    action = ProcessingAction.DROP
                else:
                    action = ProcessingAction.PASS  # Let downstream decide
                
                return ProcessingResult(
                    action=action,
                    data=data,
                    metadata={"validation_report": validation_report},
                    errors=[f"Data quality below threshold: {validation_report.data_quality}"],
                    processing_time_ms=(time.time() - start_time) * 1000,
                    stage=ProcessingStage.VALIDATION
                )
            
            # Update data quality in metadata
            if hasattr(data, 'metadata') and data.metadata:
                data.metadata.quality = validation_report.data_quality
            
            if self.config.enable_metrics:
                self.processed_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.PASS,
                data=data,
                metadata={"validation_report": validation_report},
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.VALIDATION
            )
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            if self.config.enable_metrics:
                self.error_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.DROP,
                errors=[str(e)],
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.VALIDATION
            )


class TransformationProcessor(BaseProcessor):
    """Data transformation processor."""
    
    def __init__(self, config: ProcessorConfig):
        super().__init__(config, "transformer")
        self.transformations: List[Callable[[MarketData], MarketData]] = []
        self._setup_default_transformations()
    
    def _setup_default_transformations(self):
        """Setup default transformations."""
        self.transformations = [
            self._normalize_timestamps,
            self._standardize_symbols,
            self._convert_currencies,
            self._calculate_derived_fields
        ]
    
    async def process(self, data: MarketData) -> ProcessingResult:
        """Transform market data."""
        start_time = time.time()
        
        try:
            transformed_data = data
            
            # Apply transformations sequentially
            for transformation in self.transformations:
                transformed_data = transformation(transformed_data)
            
            if self.config.enable_metrics:
                self.processed_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.MODIFY,
                data=transformed_data,
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.TRANSFORMATION
            )
            
        except Exception as e:
            self.logger.error(f"Transformation error: {e}")
            if self.config.enable_metrics:
                self.error_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.DROP,
                errors=[str(e)],
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.TRANSFORMATION
            )
    
    def _normalize_timestamps(self, data: MarketData) -> MarketData:
        """Normalize timestamps to UTC."""
        if hasattr(data, 'timestamp') and data.timestamp:
            if data.timestamp.tzinfo is None:
                # Assume UTC if no timezone info
                data.timestamp = data.timestamp.replace(tzinfo=timezone.utc)
            else:
                # Convert to UTC
                data.timestamp = data.timestamp.astimezone(timezone.utc)
        
        return data
    
    def _standardize_symbols(self, data: MarketData) -> MarketData:
        """Standardize symbol format."""
        if hasattr(data, 'symbol') and data.symbol:
            # Uppercase ticker
            data.symbol = replace(data.symbol, ticker=data.symbol.ticker.upper().strip())
            
            # Remove common prefixes/suffixes
            ticker = data.symbol.ticker
            if ticker.endswith('.US'):
                data.symbol = replace(data.symbol, ticker=ticker[:-3])
        
        return data
    
    def _convert_currencies(self, data: MarketData) -> MarketData:
        """Convert currencies if needed (placeholder for future implementation)."""
        # TODO: Implement currency conversion logic
        return data
    
    def _calculate_derived_fields(self, data: MarketData) -> MarketData:
        """Calculate derived fields."""
        if isinstance(data, Quote):
            # Calculate mid price and spread if both bid/ask are available
            if data.bid_price and data.ask_price:
                # These are already properties in the Quote class
                pass
        
        return data


class EnrichmentProcessor(BaseProcessor):
    """Data enrichment processor."""
    
    def __init__(self, config: ProcessorConfig):
        super().__init__(config, "enricher")
        self.symbol_cache: Dict[str, Dict[str, Any]] = {}
        self.exchange_mapping = self._load_exchange_mapping()
    
    def _load_exchange_mapping(self) -> Dict[str, str]:
        """Load exchange mapping data."""
        # Placeholder for exchange mapping
        return {
            "NASDAQ": "NASDAQ",
            "NYSE": "NYSE",
            "AMEX": "AMEX"
        }
    
    async def process(self, data: MarketData) -> ProcessingResult:
        """Enrich market data with additional information."""
        start_time = time.time()
        
        try:
            enriched_data = await self._enrich_symbol_info(data)
            enriched_data = await self._add_market_context(enriched_data)
            
            if self.config.enable_metrics:
                self.processed_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.MODIFY,
                data=enriched_data,
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.ENRICHMENT
            )
            
        except Exception as e:
            self.logger.error(f"Enrichment error: {e}")
            if self.config.enable_metrics:
                self.error_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.PASS,  # Don't drop on enrichment failure
                data=data,
                errors=[str(e)],
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.ENRICHMENT
            )
    
    async def _enrich_symbol_info(self, data: MarketData) -> MarketData:
        """Enrich with symbol information."""
        if hasattr(data, 'symbol') and data.symbol:
            ticker = data.symbol.ticker
            
            # Get cached symbol info or fetch if not cached
            if ticker not in self.symbol_cache:
                symbol_info = await self._fetch_symbol_info(ticker)
                self.symbol_cache[ticker] = symbol_info
            
            symbol_info = self.symbol_cache[ticker]
            
            # Enrich symbol with additional info
            if not data.symbol.exchange and 'exchange' in symbol_info:
                data.symbol = replace(data.symbol, exchange=symbol_info['exchange'])
            
            if not data.symbol.asset_class and 'asset_class' in symbol_info:
                data.symbol = replace(data.symbol, asset_class=symbol_info['asset_class'])
        
        return data
    
    async def _fetch_symbol_info(self, ticker: str) -> Dict[str, Any]:
        """Fetch symbol information (placeholder for actual implementation)."""
        # Placeholder implementation
        return {
            'exchange': 'NASDAQ' if ticker in ['AAPL', 'GOOGL', 'MSFT'] else 'NYSE',
            'asset_class': 'equity',
            'sector': 'Technology',
            'industry': 'Software'
        }
    
    async def _add_market_context(self, data: MarketData) -> MarketData:
        """Add market context information."""
        if hasattr(data, 'metadata') and data.metadata:
            # Add market hours context
            if hasattr(data, 'timestamp'):
                market_session = self._get_market_session(data.timestamp)
                data.metadata.raw_data = data.metadata.raw_data or {}
                data.metadata.raw_data['market_session'] = market_session
        
        return data
    
    def _get_market_session(self, timestamp: datetime) -> str:
        """Determine market session (regular, pre-market, after-hours)."""
        # Simple implementation - would be more complex in reality
        hour = timestamp.hour
        if 9 <= hour < 16:
            return "regular"
        elif 4 <= hour < 9:
            return "pre_market"
        else:
            return "after_hours"


class AggregationProcessor(BaseProcessor):
    """Data aggregation processor for creating bars from trades."""
    
    def __init__(self, config: ProcessorConfig):
        super().__init__(config, "aggregator")
        self.trade_buffers: Dict[str, List[Trade]] = defaultdict(list)
        self.bar_intervals = [60, 300, 900, 3600]  # 1m, 5m, 15m, 1h in seconds
        self.last_bar_times: Dict[str, Dict[int, datetime]] = defaultdict(dict)
    
    async def process(self, data: MarketData) -> ProcessingResult:
        """Aggregate trades into bars."""
        start_time = time.time()
        
        try:
            if not isinstance(data, Trade):
                # Pass through non-trade data
                return ProcessingResult(
                    action=ProcessingAction.PASS,
                    data=data,
                    processing_time_ms=(time.time() - start_time) * 1000,
                    stage=ProcessingStage.AGGREGATION
                )
            
            symbol_key = str(data.symbol)
            self.trade_buffers[symbol_key].append(data)
            
            # Check if we need to create bars
            bars = []
            for interval in self.bar_intervals:
                bar = await self._maybe_create_bar(symbol_key, interval, data.timestamp)
                if bar:
                    bars.append(bar)
            
            if bars:
                # Return the bars (for now, just return the first one)
                if self.config.enable_metrics:
                    self.processed_counter.record()
                
                return ProcessingResult(
                    action=ProcessingAction.MODIFY,
                    data=bars[0],  # Return first bar, others could be sent separately
                    metadata={"additional_bars": bars[1:]},
                    processing_time_ms=(time.time() - start_time) * 1000,
                    stage=ProcessingStage.AGGREGATION
                )
            else:
                # No bars created, pass through the trade
                return ProcessingResult(
                    action=ProcessingAction.PASS,
                    data=data,
                    processing_time_ms=(time.time() - start_time) * 1000,
                    stage=ProcessingStage.AGGREGATION
                )
                
        except Exception as e:
            self.logger.error(f"Aggregation error: {e}")
            if self.config.enable_metrics:
                self.error_counter.record()
            
            return ProcessingResult(
                action=ProcessingAction.PASS,  # Don't drop on aggregation failure
                data=data,
                errors=[str(e)],
                processing_time_ms=(time.time() - start_time) * 1000,
                stage=ProcessingStage.AGGREGATION
            )
    
    async def _maybe_create_bar(self, symbol_key: str, interval_seconds: int, 
                              current_time: datetime) -> Optional[Bar]:
        """Create bar if interval has elapsed."""
        # Determine bar timestamp (aligned to interval)
        timestamp_seconds = int(current_time.timestamp())
        bar_timestamp_seconds = (timestamp_seconds // interval_seconds) * interval_seconds
        bar_timestamp = datetime.fromtimestamp(bar_timestamp_seconds, tz=timezone.utc)
        
        # Check if we need a new bar
        last_bar_time = self.last_bar_times[symbol_key].get(interval_seconds)
        if last_bar_time and last_bar_time >= bar_timestamp:
            return None  # Bar for this interval already created
        
        # Get trades for this bar
        trades = [
            trade for trade in self.trade_buffers[symbol_key]
            if bar_timestamp <= trade.timestamp < bar_timestamp + timedelta(seconds=interval_seconds)
        ]
        
        if not trades:
            return None
        
        # Create bar from trades
        prices = [trade.price for trade in trades]
        volumes = [trade.size for trade in trades]
        
        bar = Bar(
            symbol=trades[0].symbol,
            timestamp=bar_timestamp,
            timeframe=f"{interval_seconds}s",
            open_price=prices[0],
            high_price=max(prices),
            low_price=min(prices),
            close_price=prices[-1],
            volume=sum(volumes),
            trade_count=len(trades),
            metadata=MarketDataMetadata(
                source=DataSource.CSV_FILE,  # Derived data
                source_timestamp=current_time,
                quality=DataQuality.HIGH
            )
        )
        
        # Update last bar time
        self.last_bar_times[symbol_key][interval_seconds] = bar_timestamp
        
        # Clean up old trades
        self.trade_buffers[symbol_key] = [
            trade for trade in self.trade_buffers[symbol_key]
            if trade.timestamp >= bar_timestamp
        ]
        
        return bar


class ProcessorPipeline:
    """Pipeline for chaining multiple processors."""
    
    def __init__(self, processors: List[BaseProcessor]):
        self.processors = sorted(processors, key=lambda p: p.config.priority)
        self.logger = get_logger(self.__class__.__name__)
        self.metrics_registry = MetricRegistry("pipeline")
        
        # Pipeline metrics
        self.pipeline_timer = self.metrics_registry.timer("pipeline_duration")
        self.pipeline_counter = self.metrics_registry.counter("pipeline_processed_total")
        self.pipeline_errors = self.metrics_registry.counter("pipeline_errors_total")
    
    async def process(self, data: MarketData) -> Optional[MarketData]:
        """Process data through the entire pipeline."""
        with self.pipeline_timer.time():
            current_data = data
            
            for processor in self.processors:
                if not processor.config.enabled:
                    continue
                
                try:
                    result = await processor.process(current_data)
                    
                    if result.action == ProcessingAction.DROP:
                        self.logger.debug(f"Data dropped by {processor.name}: {result.errors}")
                        return None
                    elif result.action == ProcessingAction.MODIFY and result.data:
                        current_data = result.data
                    elif result.action == ProcessingAction.DEAD_LETTER:
                        self.logger.warning(f"Data sent to dead letter queue by {processor.name}")
                        # TODO: Send to dead letter queue
                        return None
                    # PASS action continues with original data
                    
                except Exception as e:
                    self.logger.error(f"Error in processor {processor.name}: {e}")
                    self.pipeline_errors.record()
                    
                    if not processor.config.drop_invalid_data:
                        continue  # Continue with original data
                    else:
                        return None  # Drop data
            
            self.pipeline_counter.record()
            return current_data
    
    async def process_batch(self, data_batch: List[MarketData]) -> List[MarketData]:
        """Process a batch of data through the pipeline."""
        results = []
        
        # Process each item through the pipeline
        for data in data_batch:
            result = await self.process(data)
            if result:
                results.append(result)
        
        return results
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get pipeline metrics."""
        metrics = {
            "pipeline_stats": self.pipeline_timer.get_value(),
            "total_processed": self.pipeline_counter.get_value(),
            "total_errors": self.pipeline_errors.get_value(),
            "processors": {}
        }
        
        for processor in self.processors:
            metrics["processors"][processor.name] = processor.get_metrics()
        
        return metrics
    
    def add_processor(self, processor: BaseProcessor) -> None:
        """Add processor to pipeline."""
        self.processors.append(processor)
        self.processors.sort(key=lambda p: p.config.priority)
    
    def remove_processor(self, name: str) -> bool:
        """Remove processor from pipeline."""
        for i, processor in enumerate(self.processors):
            if processor.name == name:
                del self.processors[i]
                return True
        return False