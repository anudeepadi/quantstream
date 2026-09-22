"""
Bronze layer streaming job for ingesting raw data from Kafka topics.

The Bronze layer represents the raw, unprocessed data ingested from various sources.
This layer maintains data lineage and provides the foundation for downstream processing.
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, 
    from_json, when, lit, expr, hash
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import structlog

from .base_streaming_job import BaseStreamingJob
from ..transformations.data_validation import DataValidator
from ..utils.schema_registry import SchemaRegistry

logger = structlog.get_logger(__name__)


class BronzeLayerJob(BaseStreamingJob):
    """
    Bronze layer streaming job for raw data ingestion.
    
    Features:
    - Consumes from multiple Kafka topics
    - Maintains data lineage and metadata
    - Basic schema validation
    - Error handling and dead letter processing
    - Exactly-once processing guarantees
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Bronze layer job.
        
        Args:
            config: Job configuration including Kafka settings and schemas
        """
        super().__init__(config, "bronze-layer")
        self.kafka_config = config.get("kafka", {})
        self.schema_registry = SchemaRegistry(config.get("schema_registry", {}))
        self.data_validator = DataValidator(config.get("validation", {}))
        
        # Configure topics to consume from
        self.topics = self._get_topics_to_consume()
        self.schemas = self._load_schemas()
        
    def _get_topics_to_consume(self) -> list:
        """Get list of Kafka topics to consume from."""
        topics_config = self.config.get("bronze_layer", {}).get("topics", [])
        return [topic["name"] for topic in topics_config if topic.get("enabled", True)]
        
    def _load_schemas(self) -> Dict[str, StructType]:
        """Load schemas for different data types."""
        schemas = {}
        
        # Market data quote schema
        quote_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("bid_price", StringType(), True),
            StructField("ask_price", StringType(), True),
            StructField("bid_size", StringType(), True),
            StructField("ask_size", StringType(), True),
            StructField("last_price", StringType(), True),
            StructField("last_size", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("data_source", StringType(), False)
        ])
        
        # Market data trade schema
        trade_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("price", StringType(), False),
            StructField("size", StringType(), False),
            StructField("exchange", StringType(), True),
            StructField("conditions", StringType(), True),
            StructField("data_source", StringType(), False)
        ])
        
        # Market data bar schema
        bar_schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("open", StringType(), False),
            StructField("high", StringType(), False),
            StructField("low", StringType(), False),
            StructField("close", StringType(), False),
            StructField("volume", StringType(), False),
            StructField("timeframe", StringType(), False),
            StructField("data_source", StringType(), False)
        ])
        
        schemas["market_data_quotes"] = quote_schema
        schemas["market_data_trades"] = trade_schema  
        schemas["market_data_bars"] = bar_schema
        
        return schemas
        
    def create_source_stream(self) -> DataFrame:
        """
        Create Kafka source stream with proper configuration.
        
        Returns:
            DataFrame: Kafka streaming DataFrame
        """
        try:
            kafka_options = {
                "kafka.bootstrap.servers": ",".join(
                    self.kafka_config.get("bootstrap_servers", ["localhost:9092"])
                ),
                "subscribe": ",".join(self.topics),
                "startingOffsets": self.config.get("bronze_layer", {}).get("starting_offsets", "latest"),
                "maxOffsetsPerTrigger": self.config.get("bronze_layer", {}).get("max_offsets_per_trigger", 10000),
                "kafka.security.protocol": self.kafka_config.get("security_protocol", "PLAINTEXT"),
                "failOnDataLoss": "false",
                "kafka.session.timeout.ms": "30000",
                "kafka.request.timeout.ms": "40000",
                "kafka.max.poll.interval.ms": "300000"
            }
            
            # Add authentication if configured
            if self.kafka_config.get("security_protocol") in ["SASL_SSL", "SASL_PLAINTEXT"]:
                kafka_options.update({
                    "kafka.sasl.mechanism": self.kafka_config.get("sasl_mechanism", "PLAIN"),
                    "kafka.sasl.jaas.config": self.kafka_config.get("sasl_jaas_config")
                })
                
            self.logger.info("Creating Kafka source stream", 
                           topics=self.topics, 
                           bootstrap_servers=kafka_options["kafka.bootstrap.servers"])
            
            df = (
                self.spark
                .readStream
                .format("kafka")
                .options(**kafka_options)
                .load()
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to create Kafka source stream", error=str(e))
            raise
            
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Transform raw Kafka data into Bronze layer format.
        
        Args:
            df: Raw Kafka DataFrame
            
        Returns:
            DataFrame: Transformed Bronze layer DataFrame
        """
        try:
            # Add metadata columns
            df_with_metadata = df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("raw_data"),
                current_timestamp().alias("ingestion_timestamp"),
                lit(self.job_name).alias("ingestion_job"),
                hash(col("value")).alias("data_hash")
            )
            
            # Parse known topics into one compatible struct. Replacing the struct
            # per topic both erased earlier rows and mixed incompatible schemas.
            fields = {field.name: field for schema in self.schemas.values()
                      for field in schema.fields}
            combined_schema = StructType(list(fields.values()))
            df_parsed = df_with_metadata.withColumn(
                "parsed_data",
                when(col("topic").isin(list(self.schemas)),
                     from_json(col("raw_data"), combined_schema))
            )
            for name in fields:
                df_parsed = df_parsed.withColumn(name, col(f"parsed_data.{name}"))

            # Add data quality flags
            df_with_quality = self._add_data_quality_flags(df_parsed)
            
            # Add partitioning columns
            df_final = df_with_quality.withColumn(
                "year", expr("year(to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss'))")
            ).withColumn(
                "month", expr("month(to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss'))")
            ).withColumn(
                "day", expr("day(to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss'))")
            ).withColumn(
                "hour", expr("hour(to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss'))")
            )
            
            self.logger.info("Applied Bronze layer transformations")
            return df_final
            
        except Exception as e:
            self.logger.error("Failed to transform data", error=str(e))
            raise
            
    def _add_data_quality_flags(self, df: DataFrame) -> DataFrame:
        """
        Add data quality validation flags.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            DataFrame: DataFrame with quality flags
        """
        # Basic validation rules
        df_with_flags = df.withColumn(
            "is_valid_symbol",
            when(col("symbol").isNotNull() & (col("symbol") != ""), True).otherwise(False)
        ).withColumn(
            "is_valid_timestamp", 
            when(col("timestamp").isNotNull() & (col("timestamp") != ""), True).otherwise(False)
        ).withColumn(
            "is_duplicate",
            # Mark as potential duplicate if same data_hash exists
            lit(False)  # Simplified - would need window function for real duplicate detection
        )
        
        # Overall quality score
        df_with_score = df_with_flags.withColumn(
            "quality_score",
            (col("is_valid_symbol").cast("int") + 
             col("is_valid_timestamp").cast("int")) / 2.0
        ).withColumn(
            "is_high_quality",
            col("quality_score") >= 0.8
        )
        
        return df_with_score
        
    def get_sink_options(self) -> Dict[str, Any]:
        """Get Bronze layer sink configuration."""
        bronze_config = self.config.get("bronze_layer", {})
        
        return {
            "path": bronze_config.get("output_path", "/tmp/bronze"),
            "format": "delta",
            "output_mode": "append",
            "trigger_interval": bronze_config.get("trigger_interval", "30 seconds"),
            "options": {
                "mergeSchema": "true",
                "autoOptimize": "true",
                "optimizeWrite": "true",
                # Partitioning configuration
                "partitionBy": "topic,year,month,day",
                # Z-ordering for better query performance
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true"
            }
        }
        
    def optimize_bronze_table(self, table_path: str) -> None:
        """
        Optimize Bronze layer table for better query performance.
        
        Args:
            table_path: Path to the Bronze layer Delta table
        """
        try:
            self.logger.info("Starting Bronze table optimization", table_path=table_path)
            
            # Run OPTIMIZE with Z-ORDER
            optimize_sql = f"""
            OPTIMIZE delta.`{table_path}`
            ZORDER BY (symbol, kafka_timestamp, topic)
            """
            
            self.spark.sql(optimize_sql)
            
            # Run VACUUM to clean up old files (retain 7 days)
            vacuum_sql = f"""
            VACUUM delta.`{table_path}` RETAIN 168 HOURS
            """
            
            self.spark.sql(vacuum_sql)
            
            self.logger.info("Bronze table optimization completed")
            
        except Exception as e:
            self.logger.error("Failed to optimize Bronze table", error=str(e))
            raise