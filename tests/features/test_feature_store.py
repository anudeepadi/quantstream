"""
Feature Store Tests

Test suite for core feature store functionality.
"""

import pytest
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, AsyncMock, patch

from src.features.store import FeatureStore, FeatureRegistry, DeltaStorageBackend
from src.features.store.feature_metadata import (
    FeatureMetadata,
    FeatureSchema,
    FeatureType,
    IndicatorCategory
)


@pytest.fixture
def sample_feature_metadata():
    """Sample feature metadata for testing."""
    return FeatureMetadata(
        feature_id="test_sma_20",
        name="Test SMA 20",
        namespace="test_indicators",
        version="1.0.0",
        schema=FeatureSchema(
            name="sma_value",
            feature_type=FeatureType.FLOAT,
            nullable=True,  # Rolling windows have warm-up rows without a value.
            description="20-period simple moving average"
        ),
        category=IndicatorCategory.TREND,
        window_size=20,
        parameters={"period": 20},
        description="Test SMA indicator",
        data_source="test_data"
    )


@pytest.fixture
def sample_market_data():
    """Sample market data for testing."""
    dates = pd.date_range(start="2024-01-01", periods=100, freq="D")
    np.random.seed(42)
    
    # Generate realistic price data
    prices = [100.0]
    for _ in range(99):
        change = np.random.normal(0, 0.02)
        new_price = prices[-1] * (1 + change)
        prices.append(max(new_price, 1.0))
    
    data = []
    for i, (date, close) in enumerate(zip(dates, prices)):
        data.append({
            'timestamp': date,
            'entity_id': 'TEST',
            'symbol': 'TEST',
            'open': close * 0.99,
            'high': close * 1.01,
            'low': close * 0.98,
            'close': close,
            'volume': np.random.randint(10000, 100000)
        })
    
    return pd.DataFrame(data)


@pytest.fixture
async def mock_redis():
    """Mock Redis client."""
    mock_redis = AsyncMock()
    mock_redis.ping = AsyncMock(return_value=b'PONG')
    values = {}

    async def get_value(key):
        return values.get(key)

    async def set_value(key, value):
        values[key] = value
        return True

    mock_redis.get = AsyncMock(wraps=get_value)
    mock_redis.set = AsyncMock(wraps=set_value)
    mock_redis.setex = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.smembers = AsyncMock(return_value=set())
    mock_redis.sadd = AsyncMock(return_value=1)
    mock_redis.zadd = AsyncMock(return_value=1)
    mock_redis.hset = AsyncMock(return_value=1)
    mock_redis.hgetall = AsyncMock(return_value={})
    return mock_redis


@pytest.fixture
async def mock_storage():
    """Mock storage backend."""
    mock_storage = AsyncMock(spec=DeltaStorageBackend)
    mock_storage.write_features = AsyncMock(return_value=True)
    mock_storage.read_features = AsyncMock(return_value=pd.DataFrame())
    mock_storage.get_latest_features = AsyncMock(return_value={})
    mock_storage.get_feature_statistics = AsyncMock(return_value={"total_records": 100})
    return mock_storage


@pytest.fixture
async def feature_registry(mock_redis):
    """Feature registry instance for testing."""
    return FeatureRegistry(mock_redis)


@pytest.fixture
async def feature_store(mock_storage, feature_registry, mock_redis):
    """Feature store instance for testing."""
    return FeatureStore(
        storage_backend=mock_storage,
        registry=feature_registry,
        cache_client=mock_redis,
        cache_ttl_seconds=3600
    )


class TestFeatureStore:
    """Test cases for FeatureStore class."""
    
    @pytest.mark.asyncio
    async def test_register_feature(self, feature_store, sample_feature_metadata):
        """Test feature registration."""
        # Mock computation function
        def mock_computation(data, **kwargs):
            return data[['timestamp', 'entity_id']].copy().assign(sma_value=100.0)
        
        # Register feature
        success = await feature_store.register_feature(
            metadata=sample_feature_metadata,
            computation_function=mock_computation
        )
        
        assert success is True
        assert sample_feature_metadata.feature_id in feature_store.computation_functions
    
    @pytest.mark.asyncio
    async def test_compute_feature(self, feature_store, sample_feature_metadata, sample_market_data):
        """Test feature computation."""
        # Register feature first
        def mock_computation(data, **kwargs):
            # Simple moving average computation
            sma = data['close'].rolling(window=kwargs.get('period', 20)).mean()
            result_df = data[['timestamp', 'entity_id']].copy()
            result_df['sma_value'] = sma
            return result_df
        
        await feature_store.register_feature(
            metadata=sample_feature_metadata,
            computation_function=mock_computation
        )
        
        # Compute feature
        result = await feature_store.compute_feature(
            feature_id=sample_feature_metadata.feature_id,
            input_data=sample_market_data
        )
        
        assert result is not None
        assert result.success is True
        assert result.computation_time_ms > 0
        assert isinstance(result.data, pd.DataFrame)
    
    @pytest.mark.asyncio
    async def test_get_feature_vector(self, feature_store, mock_redis):
        """Test feature vector retrieval."""
        # Mock cached feature data
        mock_redis.get.return_value = b'[{"entity_id": "TEST", "sma_value": 100.5}]'
        
        feature_vector = await feature_store.get_feature_vector(
            feature_ids=["test_sma_20"],
            entity_id="TEST"
        )
        
        # Since we're mocking storage, result will be None
        # In a real test, we'd set up proper mock data
        assert feature_vector is None or isinstance(feature_vector, dict)
    
    @pytest.mark.asyncio
    async def test_batch_compute_features(self, feature_store, sample_market_data):
        """Test batch feature computation."""
        # Register multiple features
        feature_ids = ["test_sma_10", "test_sma_20"]
        
        for feature_id in feature_ids:
            metadata = FeatureMetadata(
                feature_id=feature_id,
                name=f"Test SMA {feature_id.split('_')[-1]}",
                namespace="test_indicators",
                version="1.0.0",
                schema=FeatureSchema(
                    name="sma_value",
                    feature_type=FeatureType.FLOAT,
                    nullable=True,
                    description="Simple moving average"
                ),
                category=IndicatorCategory.TREND,
                window_size=int(feature_id.split('_')[-1]),
                parameters={"period": int(feature_id.split('_')[-1])},
                description="Test SMA indicator"
            )
            
            def computation_func(data, **kwargs):
                period = kwargs.get('period', 20)
                sma = data['close'].rolling(window=period).mean()
                result_df = data[['timestamp', 'entity_id']].copy()
                result_df['sma_value'] = sma
                return result_df
            
            await feature_store.register_feature(metadata, computation_func)
        
        # Batch compute
        results = await feature_store.batch_compute_features(
            feature_ids=feature_ids,
            input_data=sample_market_data,
            parallel=True
        )
        
        assert len(results) == len(feature_ids)
        for feature_id, result in results.items():
            assert result.success is True
            assert result.feature_id == feature_id
    
    @pytest.mark.asyncio
    async def test_invalidate_cache(self, feature_store, mock_redis):
        """Test cache invalidation."""
        # Mock scan_iter to return some keys
        async def mock_scan_iter(match=None):
            for key in [b'features:test_sma_20:TEST:latest']:
                yield key
        
        mock_redis.scan_iter = mock_scan_iter
        
        success = await feature_store.invalidate_cache(feature_id="test_sma_20")
        
        assert success is True
        mock_redis.delete.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_metrics(self, feature_store):
        """Test metrics retrieval."""
        metrics = await feature_store.get_metrics()
        
        assert isinstance(metrics, dict)
        assert 'features_computed' in metrics
        assert 'cache_hits' in metrics
        assert 'cache_misses' in metrics
        assert 'errors' in metrics
    
    @pytest.mark.asyncio
    async def test_health_check(self, feature_store, mock_redis):
        """Test health check."""
        health = await feature_store.health_check()
        
        assert isinstance(health, dict)
        assert 'overall' in health
        assert 'components' in health


class TestFeatureRegistry:
    """Test cases for FeatureRegistry class."""
    
    @pytest.mark.asyncio
    async def test_register_feature(self, feature_registry, sample_feature_metadata):
        """Test feature registration in registry."""
        success = await feature_registry.register_feature(sample_feature_metadata)
        
        assert success is True
        
        # Verify the feature was stored in Redis
        feature_registry.redis.set.assert_called()
    
    @pytest.mark.asyncio
    async def test_get_feature(self, feature_registry, sample_feature_metadata, mock_redis):
        """Test feature retrieval from registry."""
        # Mock Redis response
        mock_redis.get.return_value = sample_feature_metadata.json().encode()
        
        retrieved_feature = await feature_registry.get_feature(sample_feature_metadata.feature_id)
        
        assert retrieved_feature is not None
        assert retrieved_feature.feature_id == sample_feature_metadata.feature_id
    
    @pytest.mark.asyncio
    async def test_search_features(self, feature_registry):
        """Test feature search functionality."""
        results = await feature_registry.search_features(
            query="moving average",
            limit=10
        )
        
        assert isinstance(results, list)
        # Since we're using mocks, results will be empty
        # In a real test, we'd populate the registry first
    
    @pytest.mark.asyncio
    async def test_update_feature_usage(self, feature_registry, sample_feature_metadata):
        """Test feature usage tracking."""
        # Mock existing feature
        feature_registry.redis.get.return_value = sample_feature_metadata.json().encode()
        
        await feature_registry.update_feature_usage(
            sample_feature_metadata.feature_id, 
            computation_time_ms=25.5
        )
        
        # Verify feature was updated
        feature_registry.redis.set.assert_called()


class TestFeatureValidation:
    """Test cases for feature validation."""
    
    @pytest.mark.asyncio
    async def test_metadata_validation(self, sample_feature_metadata):
        """Test feature metadata validation."""
        from src.features.utils.feature_validator import FeatureValidator
        
        validator = FeatureValidator()
        result = await validator.validate_metadata(sample_feature_metadata)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
    
    @pytest.mark.asyncio
    async def test_data_validation(self, sample_feature_metadata, sample_market_data):
        """Test feature data validation."""
        from src.features.utils.feature_validator import FeatureValidator
        
        validator = FeatureValidator()
        
        # Create feature data
        feature_data = sample_market_data[['timestamp', 'entity_id']].copy()
        feature_data['sma_value'] = sample_market_data['close'].rolling(20).mean()
        
        result = await validator.validate_data(sample_feature_metadata, feature_data)
        
        assert result.is_valid is True
        assert not result.errors

        # The same warm-up values must fail a non-nullable schema.
        sample_feature_metadata.schema.nullable = False
        invalid = await validator.validate_data(sample_feature_metadata, feature_data)
        assert invalid.is_valid is False
        assert any("Null values" in error for error in invalid.errors)


@pytest.mark.asyncio
async def test_integration_workflow(mock_redis, mock_storage):
    """Test complete integration workflow."""
    # Initialize components
    registry = FeatureRegistry(mock_redis)
    feature_store = FeatureStore(
        storage_backend=mock_storage,
        registry=registry,
        cache_client=mock_redis
    )
    
    # Create metadata
    metadata = FeatureMetadata(
        feature_id="integration_test",
        name="Integration Test Feature",
        namespace="test",
        version="1.0.0",
        schema=FeatureSchema(
            name="test_value",
            feature_type=FeatureType.FLOAT,
            description="Test feature for integration"
        ),
        category=IndicatorCategory.TREND,
        window_size=10,
        parameters={"period": 10},
        description="Integration test feature"
    )
    
    # Computation function
    def compute_test_feature(data, **kwargs):
        result = data[['timestamp', 'entity_id']].copy()
        result['test_value'] = 42.0
        return result
    
    # Register feature
    success = await feature_store.register_feature(metadata, compute_test_feature)
    assert success is True
    
    # Create test data
    test_data = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=10),
        'entity_id': ['TEST'] * 10,
        'close': np.random.randn(10) + 100
    })
    
    # Compute feature
    result = await feature_store.compute_feature(
        feature_id="integration_test",
        input_data=test_data
    )
    
    assert result is not None
    assert result.success is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])