"""
Comprehensive tests for streaming ETL jobs.
"""

import pytest
import tempfile
import shutil
import os
import sys
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import lit, current_timestamp
import pandas as pd

from src.etl.streaming.bronze_layer import BronzeLayerJob
from src.etl.streaming.silver_layer import SilverLayerJob
from src.etl.streaming.gold_layer import GoldLayerJob
from src.etl.streaming.technical_indicators import TechnicalIndicatorsJob
from src.etl.streaming.anomaly_detection import AnomalyDetectionJob


@pytest.fixture(scope="session", autouse=True)
def spark_session():
    """Create a Spark session for testing."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (SparkSession.builder
             .appName("ETL-Tests")
             .master("local[2]")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_config(temp_dir):
    """Create sample configuration for testing."""
    return {
        "kafka": {
            "bootstrap_servers": ["localhost:9092"],
            "security_protocol": "PLAINTEXT"
        },
        "bronze_layer": {
            "output_path": f"{temp_dir}/bronze",
            "checkpoint_path": f"{temp_dir}/checkpoints/bronze",
            "trigger_interval": "10 seconds",
            "topics": [
                {"name": "market_data_quotes", "enabled": True},
                {"name": "market_data_trades", "enabled": True}
            ]
        },
        "silver_layer": {
            "output_path": f"{temp_dir}/silver",
            "checkpoint_path": f"{temp_dir}/checkpoints/silver",
            "trigger_interval": "30 seconds",
            "quality_thresholds": {
                "min_price": 0.01,
                "max_price": 10000.0,
                "max_spread_pct": 5.0
            }
        },
        "gold_layer": {
            "output_path": f"{temp_dir}/gold",
            "checkpoint_path": f"{temp_dir}/checkpoints/gold",
            "trigger_interval": "1 minute",
            "time_windows": ["1 minute", "5 minutes"],
            "aggregation_types": ["ohlcv_bars"]
        },
        "technical_indicators": {
            "output_path": f"{temp_dir}/indicators",
            "checkpoint_path": f"{temp_dir}/checkpoints/indicators",
            "moving_averages": {
                "periods": [5, 10, 20],
                "types": ["sma", "ema"]
            }
        },
        "anomaly_detection": {
            "output_path": f"{temp_dir}/anomalies",
            "checkpoint_path": f"{temp_dir}/checkpoints/anomalies",
            "algorithms": {
                "statistical_outliers": {
                    "enabled": True,
                    "threshold": 3.0
                }
            }
        },
        "checkpoint_base_path": f"{temp_dir}/checkpoints"
    }


@pytest.fixture
def sample_kafka_data(spark_session):
    """Create sample Kafka-like data for testing."""
    schema = StructType([
        StructField("topic", StringType(), False),
        StructField("partition", StringType(), False),
        StructField("offset", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("key", StringType(), True),
        StructField("value", StringType(), False)
    ])
    
    # Sample market data quotes JSON
    quote_json = '{"symbol": "AAPL", "timestamp": "2024-01-01 10:00:00", "bid_price": "150.00", "ask_price": "150.05", "data_source": "test"}'
    trade_json = '{"symbol": "AAPL", "timestamp": "2024-01-01 10:00:01", "price": "150.02", "size": "100", "data_source": "test"}'
    
    data = [
        ("market_data_quotes", "0", "100", datetime(2024, 1, 1, 10, 0, 0), "AAPL", quote_json),
        ("market_data_trades", "0", "101", datetime(2024, 1, 1, 10, 0, 1), "AAPL", trade_json)
    ]
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_silver_data(spark_session):
    """Create sample Silver layer data for testing."""
    schema = StructType([
        StructField("symbol_clean", StringType(), False),
        StructField("timestamp_parsed", TimestampType(), False),
        StructField("last_price_decimal", DoubleType(), True),
        StructField("volume_long", LongType(), True),
        StructField("is_silver_quality", StringType(), False),
        StructField("topic", StringType(), False)
    ])
    
    data = [
        ("AAPL", datetime.fromisoformat("2024-01-01 10:00:00"), 150.00, 1000, "true", "market_data_trades"),
        ("AAPL", datetime.fromisoformat("2024-01-01 10:01:00"), 150.50, 1500, "true", "market_data_trades"),
        ("AAPL", datetime.fromisoformat("2024-01-01 10:02:00"), 149.75, 800, "true", "market_data_trades")
    ]
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_ohlcv_data(spark_session):
    """Create sample OHLCV data for testing."""
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("timeframe", StringType(), False),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("aggregation_type", StringType(), False)
    ])
    
    data = [
        ("AAPL", datetime.fromisoformat("2024-01-01 10:00:00"), datetime.fromisoformat("2024-01-01 10:01:00"), "1 minute", 150.00, 150.50, 149.75, 150.25, 5000, "ohlcv_bars"),
        ("AAPL", datetime.fromisoformat("2024-01-01 10:01:00"), datetime.fromisoformat("2024-01-01 10:02:00"), "1 minute", 150.25, 150.75, 150.00, 150.60, 4500, "ohlcv_bars")
    ]
    
    return spark_session.createDataFrame(data, schema)


class TestBronzeLayerJob:
    """Test Bronze layer streaming job."""
    
    def test_bronze_job_initialization(self, sample_config):
        """Test Bronze job initialization."""
        job = BronzeLayerJob(sample_config)
        
        assert job.job_name == "bronze-layer"
        assert job.kafka_config == sample_config["kafka"]
        assert len(job.topics) == 2
        assert "market_data_quotes" in job.topics
        assert "market_data_trades" in job.topics
        
    def test_bronze_transform_data(self, sample_config, sample_kafka_data):
        """Test Bronze layer data transformation."""
        job = BronzeLayerJob(sample_config)
        
        # Transform the sample data
        result_df = job.transform_data(sample_kafka_data)
        
        # Check that metadata columns were added
        expected_columns = [
            "topic", "partition", "offset", "kafka_timestamp", 
            "raw_data", "ingestion_timestamp", "data_hash"
        ]
        
        for col in expected_columns:
            assert col in result_df.columns
            
        # Check data quality flags
        assert "is_valid_symbol" in result_df.columns
        assert "quality_score" in result_df.columns
        rows = {row.topic: row for row in result_df.collect()}
        assert rows["market_data_quotes"].symbol == "AAPL"
        assert rows["market_data_quotes"].bid_price == "150.00"
        assert rows["market_data_trades"].symbol == "AAPL"
        assert rows["market_data_trades"].price == "150.02"
        
    def test_bronze_sink_options(self, sample_config):
        """Test Bronze layer sink configuration."""
        job = BronzeLayerJob(sample_config)
        sink_options = job.get_sink_options()
        
        assert sink_options["format"] == "delta"
        assert sink_options["output_mode"] == "append"
        assert "autoOptimize" in sink_options["options"]


class TestSilverLayerJob:
    """Test Silver layer streaming job."""
    
    def test_silver_job_initialization(self, sample_config):
        """Test Silver job initialization."""
        job = SilverLayerJob(sample_config)
        
        assert job.job_name == "silver-layer"
        assert job.bronze_path == sample_config["bronze_layer"]["output_path"]
        assert job.quality_thresholds["min_price"] == 0.01
        
    @patch.object(SilverLayerJob, 'create_source_stream')
    def test_silver_cleaning_transformation(self, mock_source, sample_config, sample_kafka_data):
        """Test Silver layer data cleaning."""
        job = SilverLayerJob(sample_config)
        
        # Test the cleaning method directly
        bronze_df = BronzeLayerJob(sample_config).transform_data(sample_kafka_data)
        cleaned_df = job._clean_and_standardize(bronze_df)
        
        # Should have standardized symbol column
        assert {row.symbol_clean for row in cleaned_df.select("symbol_clean").collect()} == {"AAPL"}
            
    def test_silver_data_validation(self, sample_config, sample_kafka_data):
        """Test Silver layer data validation."""
        job = SilverLayerJob(sample_config)
        
        # Add some numeric columns for testing
        test_df = sample_kafka_data.withColumn("price_decimal", lit(150.0)).withColumn("volume_long", lit(1000))
        
        validated_df = job._validate_data_quality(test_df)
        
        # Should have validation flags
        validation_columns = [col for col in validated_df.columns if col.startswith("is_valid_")]
        assert len(validation_columns) > 0


class TestGoldLayerJob:
    """Test Gold layer streaming job."""
    
    def test_gold_job_initialization(self, sample_config):
        """Test Gold job initialization."""
        job = GoldLayerJob(sample_config)
        
        assert job.job_name == "gold-layer"
        assert "1 minute" in job.time_windows
        assert "ohlcv_bars" in job.aggregation_types
        
    def test_ohlcv_calculation(self, sample_config, sample_silver_data):
        """Test OHLCV bar calculation."""
        job = GoldLayerJob(sample_config)
        
        # Add required columns for OHLCV calculation
        trade_df = (sample_silver_data
                    .withColumn("price_decimal", sample_silver_data.last_price_decimal)
                    .withColumn("size_long", sample_silver_data.volume_long)
                    .filter(sample_silver_data.topic == "market_data_trades"))
        
        if trade_df.count() > 0:
            # Mock the OHLCV calculation
            result_df = job._create_ohlcv_bars(trade_df)
            
            # Should return either data or empty DataFrame with schema
            assert result_df is not None


class TestTechnicalIndicatorsJob:
    """Test Technical Indicators streaming job."""
    
    def test_indicators_job_initialization(self, sample_config):
        """Test Technical Indicators job initialization."""
        job = TechnicalIndicatorsJob(sample_config)
        
        assert job.job_name == "technical-indicators"
        assert 5 in job.moving_averages["periods"]
        assert "sma" in job.moving_averages["types"]
        
    def test_moving_averages_calculation(self, sample_config, sample_ohlcv_data):
        """Test moving averages calculation."""
        job = TechnicalIndicatorsJob(sample_config)
        
        result_df = job._calculate_moving_averages(sample_ohlcv_data, None)
        
        # Should have SMA columns
        sma_columns = [col for col in result_df.columns if col.startswith("sma_")]
        assert len(sma_columns) > 0
        
    def test_rsi_calculation(self, sample_config, sample_ohlcv_data):
        """Test RSI calculation."""
        job = TechnicalIndicatorsJob(sample_config)
        
        result_df = job._calculate_rsi(sample_ohlcv_data, None)
        
        # Should have RSI column
        assert "rsi" in result_df.columns


class TestAnomalyDetectionJob:
    """Test Anomaly Detection streaming job."""
    
    def test_anomaly_job_initialization(self, sample_config):
        """Test Anomaly Detection job initialization."""
        job = AnomalyDetectionJob(sample_config)
        
        assert job.job_name == "anomaly-detection"
        assert job.statistical_config["threshold"] == 3.0
        
    def test_price_spike_detection(self, sample_config, sample_silver_data):
        """Test price spike detection."""
        job = AnomalyDetectionJob(sample_config)
        
        # Add price change column
        test_df = sample_silver_data.withColumn("price_change_pct", lit(6.0))  # Above threshold
        
        result_df = job._detect_price_spikes(test_df)
        
        # Should have spike detection columns
        assert "is_price_spike" in result_df.columns
        assert "anomaly_score_price_spike" in result_df.columns
        
    def test_volume_anomaly_detection(self, sample_config, sample_silver_data):
        """Test volume anomaly detection."""
        job = AnomalyDetectionJob(sample_config)
        
        result_df = job._detect_volume_anomalies(sample_silver_data)
        
        # Should have volume anomaly columns
        assert "is_volume_spike" in result_df.columns
        assert "anomaly_score_volume" in result_df.columns
        
    def test_composite_anomaly_score(self, sample_config, sample_silver_data):
        """Test composite anomaly score calculation."""
        job = AnomalyDetectionJob(sample_config)
        
        # Add some anomaly score columns
        test_df = sample_silver_data.withColumn("anomaly_score_price", lit(0.5)).withColumn("anomaly_score_volume", lit(0.3))
        
        result_df = job._calculate_composite_anomaly_score(test_df)
        
        # Should have composite score
        assert "composite_anomaly_score" in result_df.columns


@pytest.mark.integration
class TestETLIntegration:
    """Integration tests for ETL pipeline components."""
    
    def test_bronze_to_silver_flow(self, sample_config, sample_kafka_data, temp_dir):
        """Test data flow from Bronze to Silver layer."""
        # This would be a more complex integration test
        # involving actual Delta tables and streaming
        pass
        
    def test_end_to_end_pipeline(self, sample_config, temp_dir):
        """Test complete pipeline flow."""
        # This would test the entire pipeline end-to-end
        # with real streaming data and Delta tables
        pass


@pytest.mark.performance
class TestPerformance:
    """Performance tests for ETL components."""
    
    def test_bronze_layer_throughput(self, sample_config):
        """Test Bronze layer throughput."""
        # This would test processing throughput
        # with large volumes of data
        pass
        
    def test_silver_layer_latency(self, sample_config):
        """Test Silver layer processing latency."""
        # This would test end-to-end latency
        # from ingestion to processed data
        pass


if __name__ == "__main__":
    pytest.main([__file__])
