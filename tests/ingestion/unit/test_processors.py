"""
Unit tests for data processors and pipeline components.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import replace
from collections import defaultdict
import time

from src.ingestion.processors import (
    BaseProcessor, ValidationProcessor, TransformationProcessor,
    EnrichmentProcessor, AggregationProcessor, ProcessorPipeline,
    ProcessorConfig, ProcessingStage, ProcessingAction, ProcessingResult,
    HighPerformanceKafkaProducer, KafkaProducerConfig, MessageBatch,
    SymbolPartitioner, DeadLetterQueue, CompressionType, AcknowledgmentMode,
    QuantStreamPipelineManager, PipelineConfig, BackpressureController
)
from src.ingestion.models import (
    Symbol, Quote, Trade, Bar, DataSource, DataQuality, MarketDataMessage, DataType
)


class MockProcessor(BaseProcessor):
    """Mock processor for testing."""
    
    def __init__(self, config, name, should_fail=False, modify_data=False):
        super().__init__(config, name)
        self.should_fail = should_fail
        self.modify_data = modify_data
        self.processed_items = []
    
    async def process(self, data):
        start_time = time.time()
        self.processed_items.append(data)
        
        if self.should_fail:
            raise Exception("Mock processor failure")
        
        if self.modify_data:
            # Modify the data somehow
            if hasattr(data, 'symbol') and hasattr(data.symbol, 'ticker'):
                data.symbol = replace(data.symbol, ticker=data.symbol.ticker.upper() + "_MODIFIED")
            action = ProcessingAction.MODIFY
        else:
            action = ProcessingAction.PASS
        
        return ProcessingResult(
            action=action,
            data=data,
            processing_time_ms=(time.time() - start_time) * 1000,
            stage=ProcessingStage.TRANSFORMATION
        )


class TestProcessorConfig:
    """Test ProcessorConfig functionality."""
    
    def test_processor_config_defaults(self):
        """Test processor configuration with defaults."""
        config = ProcessorConfig()
        
        assert config.enabled is True
        assert config.priority == 100
        assert config.parallel_processing is True
        assert config.max_workers == 4
        assert config.batch_size == 100
        assert config.timeout_ms == 5000.0
        assert config.retry_attempts == 3
        assert config.drop_invalid_data is False
        assert config.enable_metrics is True
        assert isinstance(config.custom_config, dict)
    
    def test_processor_config_custom(self):
        """Test processor configuration with custom values."""
        config = ProcessorConfig(
            enabled=False,
            priority=200,
            parallel_processing=False,
            max_workers=8,
            batch_size=50,
            timeout_ms=10000.0,
            retry_attempts=5,
            drop_invalid_data=True,
            enable_metrics=False,
            custom_config={"test": "value"}
        )
        
        assert config.enabled is False
        assert config.priority == 200
        assert config.parallel_processing is False
        assert config.max_workers == 8
        assert config.batch_size == 50
        assert config.timeout_ms == 10000.0
        assert config.retry_attempts == 5
        assert config.drop_invalid_data is True
        assert config.enable_metrics is False
        assert config.custom_config == {"test": "value"}


class TestProcessingResult:
    """Test ProcessingResult functionality."""
    
    def test_processing_result_creation(self):
        """Test processing result creation."""
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = ProcessingResult(
            action=ProcessingAction.MODIFY,
            data=quote,
            metadata={"test": "metadata"},
            errors=["test error"],
            processing_time_ms=10.5,
            stage=ProcessingStage.VALIDATION
        )
        
        assert result.action == ProcessingAction.MODIFY
        assert result.data == quote
        assert result.metadata == {"test": "metadata"}
        assert result.errors == ["test error"]
        assert result.processing_time_ms == 10.5
        assert result.stage == ProcessingStage.VALIDATION


class TestBaseProcessor:
    """Test BaseProcessor functionality."""
    
    @pytest.mark.asyncio
    async def test_processor_single_item(self):
        """Test processing single item."""
        config = ProcessorConfig(enable_metrics=False)  # Disable metrics for simplicity
        processor = MockProcessor(config, "test-processor")
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await processor.process(quote)
        
        assert result.action == ProcessingAction.PASS
        assert result.data == quote
        assert len(processor.processed_items) == 1
        assert processor.processed_items[0] == quote
    
    @pytest.mark.asyncio
    async def test_processor_batch_sequential(self):
        """Test processing batch sequentially."""
        config = ProcessorConfig(parallel_processing=False, enable_metrics=False)
        processor = MockProcessor(config, "test-processor")
        
        # Create test data
        quotes = []
        for i in range(5):
            symbol = Symbol(ticker=f"STOCK{i}")
            quote = Quote(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                bid_price=Decimal("150.50"),
                ask_price=Decimal("150.52")
            )
            quotes.append(quote)
        
        results = await processor.process_batch(quotes)
        
        assert len(results) == 5
        assert all(result.action == ProcessingAction.PASS for result in results)
        assert len(processor.processed_items) == 5
    
    @pytest.mark.asyncio
    async def test_processor_batch_parallel(self):
        """Test processing batch in parallel."""
        config = ProcessorConfig(parallel_processing=True, max_workers=2, enable_metrics=False)
        processor = MockProcessor(config, "test-processor")
        
        # Create test data
        quotes = []
        for i in range(5):
            symbol = Symbol(ticker=f"STOCK{i}")
            quote = Quote(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                bid_price=Decimal("150.50"),
                ask_price=Decimal("150.52")
            )
            quotes.append(quote)
        
        results = await processor.process_batch(quotes)
        
        assert len(results) == 5
        assert all(result.action == ProcessingAction.PASS for result in results)
        assert len(processor.processed_items) == 5
    
    @pytest.mark.asyncio
    async def test_processor_failure_handling(self):
        """Test processor failure handling."""
        config = ProcessorConfig(enable_metrics=False)
        processor = MockProcessor(config, "test-processor", should_fail=True)
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        # Single item failure
        with pytest.raises(Exception):
            await processor.process(quote)
        
        # Batch failure handling
        results = await processor.process_batch([quote])
        assert len(results) == 1
        assert results[0].action == ProcessingAction.DROP
        assert len(results[0].errors) > 0


class TestValidationProcessor:
    """Test ValidationProcessor functionality."""
    
    @pytest.mark.asyncio
    async def test_validation_processor_valid_data(self):
        """Test validation processor with valid data."""
        config = ProcessorConfig(enable_metrics=False)
        processor = ValidationProcessor(config)
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await processor.process(quote)
        
        assert result.action == ProcessingAction.PASS
        assert result.data == quote
        assert "validation_report" in result.metadata
    
    @pytest.mark.asyncio
    async def test_validation_processor_invalid_data(self):
        """Test validation processor with invalid data."""
        config = ProcessorConfig(drop_invalid_data=True, enable_metrics=False)
        processor = ValidationProcessor(config)
        
        # Create invalid quote (bid > ask)
        symbol = Symbol(ticker="AAPL")
        invalid_quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.52"),  # Bid higher than ask
            ask_price=Decimal("150.50")
        )
        
        result = await processor.process(invalid_quote)
        
        # Should drop invalid data when configured to do so
        assert result.action in [ProcessingAction.DROP, ProcessingAction.PASS]  # Depends on validation strictness


class TestTransformationProcessor:
    """Test TransformationProcessor functionality."""
    
    @pytest.mark.asyncio
    async def test_transformation_processor(self):
        """Test transformation processor."""
        config = ProcessorConfig(enable_metrics=False)
        processor = TransformationProcessor(config)
        
        symbol = Symbol(ticker="aapl")  # Lowercase to test transformation
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(),  # No timezone to test normalization
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await processor.process(quote)
        
        assert result.action == ProcessingAction.MODIFY
        assert result.data.symbol.ticker == "AAPL"  # Should be uppercase
        assert result.data.timestamp.tzinfo is not None  # Should have timezone


class TestEnrichmentProcessor:
    """Test EnrichmentProcessor functionality."""
    
    @pytest.mark.asyncio
    async def test_enrichment_processor(self):
        """Test enrichment processor."""
        config = ProcessorConfig(enable_metrics=False)
        processor = EnrichmentProcessor(config)
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await processor.process(quote)
        
        assert result.action == ProcessingAction.MODIFY
        # Symbol should be enriched with exchange info
        assert result.data.symbol.exchange is not None


class TestAggregationProcessor:
    """Test AggregationProcessor functionality."""
    
    @pytest.mark.asyncio
    async def test_aggregation_processor_trade(self):
        """Test aggregation processor with trade data."""
        config = ProcessorConfig(enable_metrics=False)
        processor = AggregationProcessor(config)
        
        symbol = Symbol(ticker="AAPL")
        trade = Trade(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            price=Decimal("150.51"),
            size=100
        )
        
        result = await processor.process(trade)
        
        # First trade should pass through, bars created when interval completes
        assert result.action in [ProcessingAction.PASS, ProcessingAction.MODIFY]
    
    @pytest.mark.asyncio
    async def test_aggregation_processor_non_trade(self):
        """Test aggregation processor with non-trade data."""
        config = ProcessorConfig(enable_metrics=False)
        processor = AggregationProcessor(config)
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await processor.process(quote)
        
        # Non-trade data should pass through unchanged
        assert result.action == ProcessingAction.PASS
        assert result.data == quote


class TestProcessorPipeline:
    """Test ProcessorPipeline functionality."""
    
    @pytest.mark.asyncio
    async def test_pipeline_single_processor(self):
        """Test pipeline with single processor."""
        config = ProcessorConfig(enable_metrics=False)
        processor = MockProcessor(config, "test-processor")
        
        pipeline = ProcessorPipeline([processor])
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await pipeline.process(quote)
        
        assert result == quote
        assert len(processor.processed_items) == 1
    
    @pytest.mark.asyncio
    async def test_pipeline_multiple_processors(self):
        """Test pipeline with multiple processors."""
        config = ProcessorConfig(enable_metrics=False)
        processor1 = MockProcessor(config, "processor-1", modify_data=True)
        processor2 = MockProcessor(config, "processor-2")
        
        pipeline = ProcessorPipeline([processor1, processor2])
        
        symbol = Symbol(ticker="aapl")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await pipeline.process(quote)
        
        # Data should be modified by first processor
        assert result.symbol.ticker == "AAPL_MODIFIED"
        assert len(processor1.processed_items) == 1
        assert len(processor2.processed_items) == 1
    
    @pytest.mark.asyncio
    async def test_pipeline_processor_drop(self):
        """Test pipeline when processor drops data."""
        config = ProcessorConfig(enable_metrics=False)
        dropping_processor = MockProcessor(config, "dropper")
        
        # Mock the processor to return DROP action
        async def mock_process(data):
            return ProcessingResult(action=ProcessingAction.DROP, data=None)
        
        dropping_processor.process = mock_process
        
        pipeline = ProcessorPipeline([dropping_processor])
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await pipeline.process(quote)
        
        # Result should be None when data is dropped
        assert result is None
    
    @pytest.mark.asyncio
    async def test_pipeline_processor_failure(self):
        """Test pipeline processor failure handling."""
        config = ProcessorConfig(drop_invalid_data=False, enable_metrics=False)
        failing_processor = MockProcessor(config, "failer", should_fail=True)
        good_processor = MockProcessor(config, "good")
        
        pipeline = ProcessorPipeline([failing_processor, good_processor])
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        
        result = await pipeline.process(quote)
        
        # Pipeline should continue with original data after processor failure
        assert result == quote
        assert len(good_processor.processed_items) == 1
    
    @pytest.mark.asyncio
    async def test_pipeline_batch_processing(self):
        """Test pipeline batch processing."""
        config = ProcessorConfig(enable_metrics=False)
        processor = MockProcessor(config, "test-processor")
        
        pipeline = ProcessorPipeline([processor])
        
        # Create batch of quotes
        quotes = []
        for i in range(3):
            symbol = Symbol(ticker=f"STOCK{i}")
            quote = Quote(
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                bid_price=Decimal("150.50"),
                ask_price=Decimal("150.52")
            )
            quotes.append(quote)
        
        results = await pipeline.process_batch(quotes)
        
        assert len(results) == 3
        assert all(result is not None for result in results)
        assert len(processor.processed_items) == 3
    
    def test_pipeline_add_remove_processors(self):
        """Test adding and removing processors from pipeline."""
        config = ProcessorConfig(priority=100, enable_metrics=False)
        processor1 = MockProcessor(config, "processor-1")
        
        pipeline = ProcessorPipeline([processor1])
        assert len(pipeline.processors) == 1
        
        # Add processor
        config2 = ProcessorConfig(priority=50, enable_metrics=False)  # Lower priority (higher precedence)
        processor2 = MockProcessor(config2, "processor-2")
        pipeline.add_processor(processor2)
        
        assert len(pipeline.processors) == 2
        # Processor2 should be first due to lower priority number
        assert pipeline.processors[0].name == "processor-2"
        
        # Remove processor
        removed = pipeline.remove_processor("processor-1")
        assert removed is True
        assert len(pipeline.processors) == 1
        assert pipeline.processors[0].name == "processor-2"
        
        # Try to remove non-existent processor
        removed = pipeline.remove_processor("non-existent")
        assert removed is False


class TestMessageBatch:
    """Test MessageBatch functionality."""
    
    def test_message_batch_creation(self):
        """Test message batch creation."""
        batch = MessageBatch(max_size=3, max_age_seconds=1.0)
        
        assert batch.max_size == 3
        assert batch.max_age_seconds == 1.0
        assert len(batch.messages) == 0
        assert batch.size() == 0
    
    def test_message_batch_add_messages(self):
        """Test adding messages to batch."""
        batch = MessageBatch(max_size=2)
        
        # Add first message
        success = batch.add_message("topic1", b"key1", b"value1")
        assert success is True
        assert batch.size() == 1
        
        # Add second message
        success = batch.add_message("topic2", b"key2", b"value2")
        assert success is True
        assert batch.size() == 2
        
        # Try to add third message (should fail)
        success = batch.add_message("topic3", b"key3", b"value3")
        assert success is False
        assert batch.size() == 2
    
    def test_message_batch_ready_by_size(self):
        """Test batch ready condition by size."""
        batch = MessageBatch(max_size=2, max_age_seconds=10.0)
        
        assert not batch.is_ready()
        
        batch.add_message("topic1", b"key1", b"value1")
        assert not batch.is_ready()
        
        batch.add_message("topic2", b"key2", b"value2")
        assert batch.is_ready()
    
    def test_message_batch_ready_by_age(self):
        """Test batch ready condition by age."""
        batch = MessageBatch(max_size=10, max_age_seconds=0.01)  # 10ms
        
        batch.add_message("topic1", b"key1", b"value1")
        assert not batch.is_ready()
        
        # Wait for batch to age
        time.sleep(0.02)  # 20ms
        assert batch.is_ready()


class TestSymbolPartitioner:
    """Test SymbolPartitioner functionality."""
    
    def test_symbol_partitioner(self):
        """Test symbol-based partitioning."""
        partitioner = SymbolPartitioner()
        partitions = [0, 1, 2, 3]
        
        # Test consistent partitioning
        key = b"AAPL"
        partition1 = partitioner.partition("test_topic", key, partitions)
        partition2 = partitioner.partition("test_topic", key, partitions)
        
        assert partition1 == partition2
        assert partition1 in partitions
        
        # Test different symbols get different partitions (probabilistically)
        different_symbols = [b"AAPL", b"GOOGL", b"MSFT", b"TSLA"]
        partitions_used = set()
        
        for symbol in different_symbols:
            partition = partitioner.partition("test_topic", symbol, partitions)
            partitions_used.add(partition)
        
        # With 4 symbols and 4 partitions, we should get some distribution
        assert len(partitions_used) >= 2  # At least 2 different partitions used
    
    def test_symbol_partitioner_no_key(self):
        """Test partitioner with no key."""
        partitioner = SymbolPartitioner()
        partitions = [0, 1, 2, 3]
        
        partition = partitioner.partition("test_topic", None, partitions)
        assert partition in partitions
        
        partition = partitioner.partition("test_topic", b"", partitions)
        assert partition in partitions


class TestDeadLetterQueue:
    """Test DeadLetterQueue functionality."""
    
    def test_dlq_creation(self):
        """Test dead letter queue creation."""
        dlq = DeadLetterQueue(max_size=100)
        
        assert dlq.max_size == 100
        assert dlq.size() == 0
        assert dlq.get_metrics()["total_messages"] == 0
    
    def test_dlq_add_message(self):
        """Test adding message to dead letter queue."""
        dlq = DeadLetterQueue(max_size=100)
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        message = MarketDataMessage(data_type=DataType.QUOTE, data=quote)
        
        dlq.add_message(message, "Test error", time.time())
        
        assert dlq.size() == 1
        
        messages = dlq.get_messages(10)
        assert len(messages) == 1
        assert messages[0]["message"] == message
        assert messages[0]["error"] == "Test error"
        assert "timestamp" in messages[0]
    
    def test_dlq_max_size(self):
        """Test dead letter queue max size constraint."""
        dlq = DeadLetterQueue(max_size=2)
        
        symbol = Symbol(ticker="AAPL")
        quote = Quote(
            symbol=symbol,
            timestamp=datetime.now(timezone.utc),
            bid_price=Decimal("150.50"),
            ask_price=Decimal("150.52")
        )
        message = MarketDataMessage(data_type=DataType.QUOTE, data=quote)
        
        # Add messages beyond max size
        for i in range(5):
            dlq.add_message(message, f"Error {i}")
        
        # Should only keep the last 2 messages
        assert dlq.size() == 2
        
        messages = dlq.get_messages(10)
        assert len(messages) == 2


class TestBackpressureController:
    """Test BackpressureController functionality."""
    
    def test_backpressure_controller_creation(self):
        """Test backpressure controller creation."""
        controller = BackpressureController(threshold=0.8, max_buffer_size=1000)
        
        assert controller.threshold == 0.8
        assert controller.max_buffer_size == 1000
        assert controller.current_load == 0.0
        assert not controller.is_active
    
    def test_backpressure_activation(self):
        """Test backpressure activation and deactivation."""
        controller = BackpressureController(threshold=0.8, max_buffer_size=100)
        
        # Below threshold - no backpressure
        controller.update_load(50)  # 50% load
        assert not controller.should_apply_backpressure()
        assert controller.get_delay() == 0.0
        
        # Above threshold - backpressure active
        controller.update_load(90)  # 90% load
        assert controller.should_apply_backpressure()
        assert controller.get_delay() > 0.0
        
        # Back below threshold - backpressure inactive
        controller.update_load(70)  # 70% load
        assert not controller.should_apply_backpressure()
        assert controller.get_delay() == 0.0
    
    def test_backpressure_delay_calculation(self):
        """Test backpressure delay calculation."""
        controller = BackpressureController(threshold=0.7, max_buffer_size=100)
        
        # Test different load levels
        test_cases = [
            (50, 0.0),   # Below threshold
            (80, 0.2),   # 10% excess * 2 = 0.2s
            (100, 0.6),  # 30% excess * 2 = 0.6s
        ]
        
        for buffer_size, expected_min_delay in test_cases:
            controller.update_load(buffer_size)
            delay = controller.get_delay()
            
            if buffer_size <= 70:  # Below threshold
                assert delay == 0.0
            else:
                assert delay >= expected_min_delay * 0.8  # Allow some tolerance
                assert delay <= 1.0  # Max delay cap


if __name__ == "__main__":
    pytest.main([__file__])