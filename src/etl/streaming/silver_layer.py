"""
Silver layer streaming job for data cleaning, validation, and enrichment.

The Silver layer represents cleaned, validated, and enriched data that serves
as the foundation for analytics and machine learning workloads.
"""

from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, regexp_replace, upper, trim,
    to_timestamp, cast, round as spark_round, coalesce,
    lag, lead, first, last, mean, stddev, max as spark_max, min as spark_min,
    window, count, sum as spark_sum, desc, asc, row_number
)
from pyspark.sql.types import DoubleType, DecimalType, TimestampType
from pyspark.sql.window import Window
import structlog

from .base_streaming_job import BaseStreamingJob
from ..transformations.data_cleaner import DataCleaner
from ..quality.data_quality_checker import DataQualityChecker

logger = structlog.get_logger(__name__)


class SilverLayerJob(BaseStreamingJob):
    """
    Silver layer streaming job for data cleaning and enrichment.
    
    Features:
    - Data type conversions and standardization
    - Data validation and quality checks
    - Reference data enrichment
    - Deduplication and error correction
    - Statistical outlier detection
    - Schema evolution handling
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Silver layer job.
        
        Args:
            config: Job configuration including transformation rules and quality thresholds
        """
        super().__init__(config, "silver-layer")
        self.bronze_path = config.get("bronze_layer", {}).get("output_path", "/tmp/bronze")
        self.reference_data_path = config.get("reference_data", {}).get("path", "/tmp/reference")
        
        # Initialize transformation components
        self.data_cleaner = DataCleaner(config.get("cleaning", {}))
        self.quality_checker = DataQualityChecker(config.get("quality", {}))
        
        # Quality thresholds
        self.quality_thresholds = {
            "min_price": 0.01,
            "max_price": 100000.0,
            "min_volume": 0,
            "max_volume": 1000000000,
            "max_spread_pct": 10.0,
            **config.get("silver_layer", {}).get("quality_thresholds", {})
        }
        
    def create_source_stream(self) -> DataFrame:
        """
        Create source stream from Bronze layer Delta table.
        
        Returns:
            DataFrame: Bronze layer streaming DataFrame
        """
        try:
            self.logger.info("Creating source stream from Bronze layer", 
                           bronze_path=self.bronze_path)
            
            # Read from Bronze layer with watermarking
            df = (
                self.spark
                .readStream
                .format("delta")
                .option("ignoreChanges", "true")
                .option("ignoreDeletes", "true")
                .load(self.bronze_path)
                .filter(col("is_high_quality") == True)  # Only process high-quality records
                .withWatermark("kafka_timestamp", "10 minutes")  # Handle late data
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to create Bronze layer source stream", error=str(e))
            raise
            
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply Silver layer transformations.
        
        Args:
            df: Bronze layer DataFrame
            
        Returns:
            DataFrame: Cleaned and enriched Silver layer DataFrame
        """
        try:
            # Step 1: Data cleaning and standardization
            df_cleaned = self._clean_and_standardize(df)
            
            # Step 2: Data type conversions
            df_typed = self._convert_data_types(df_cleaned)
            
            # Step 3: Data validation and quality checks
            df_validated = self._validate_data_quality(df_typed)
            
            # Step 4: Deduplication
            df_deduped = self._deduplicate_records(df_validated)
            
            # Step 5: Outlier detection and handling
            df_outliers_handled = self._handle_outliers(df_deduped)
            
            # Step 6: Data enrichment
            df_enriched = self._enrich_data(df_outliers_handled)
            
            # Step 7: Final transformations and metadata
            df_final = self._add_silver_metadata(df_enriched)
            
            self.logger.info("Applied Silver layer transformations")
            return df_final
            
        except Exception as e:
            self.logger.error("Failed to transform Silver layer data", error=str(e))
            raise
            
    def _clean_and_standardize(self, df: DataFrame) -> DataFrame:
        """
        Clean and standardize raw data fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        # Standardize symbol format
        df_cleaned = df.withColumn(
            "symbol_clean",
            upper(trim(regexp_replace(col("symbol"), "[^A-Za-z0-9]", "")))
        )
        
        # Clean numeric fields (remove non-numeric characters)
        numeric_fields = ["bid_price", "ask_price", "last_price", "price", "size", 
                         "volume", "open", "high", "low", "close"]
        
        for field in numeric_fields:
            if field in df.columns:
                df_cleaned = df_cleaned.withColumn(
                    f"{field}_clean",
                    regexp_replace(col(field), "[^0-9.-]", "")
                )
                
        return df_cleaned
        
    def _convert_data_types(self, df: DataFrame) -> DataFrame:
        """
        Convert string fields to appropriate data types.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with proper data types
        """
        # Convert timestamp
        df_typed = df.withColumn(
            "timestamp_parsed",
            to_timestamp(col("timestamp"))
        )
        
        # Convert price fields to decimal
        price_fields = ["bid_price_clean", "ask_price_clean", "last_price_clean", 
                       "price_clean", "open_clean", "high_clean", "low_clean", "close_clean"]
        
        for field in price_fields:
            if field in df.columns:
                df_typed = df_typed.withColumn(
                    field.replace("_clean", "_decimal"),
                    cast(col(field), DecimalType(18, 8))
                )
                
        # Convert volume fields to long
        volume_fields = ["volume_clean", "size_clean", "bid_size", "ask_size"]
        
        for field in volume_fields:
            if field in df.columns:
                df_typed = df_typed.withColumn(
                    field.replace("_clean", "_long"),
                    cast(col(field), "long")
                )
                
        return df_typed
        
    def _validate_data_quality(self, df: DataFrame) -> DataFrame:
        """
        Apply data quality validation rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with quality flags
        """
        df_validated = df
        
        # Price validation
        for price_field in ["bid_price_decimal", "ask_price_decimal", "last_price_decimal", "price_decimal"]:
            if price_field in df.columns:
                df_validated = df_validated.withColumn(
                    f"is_valid_{price_field}",
                    when(
                        (col(price_field).isNotNull()) &
                        (~isnan(col(price_field))) &
                        (col(price_field) > self.quality_thresholds["min_price"]) &
                        (col(price_field) < self.quality_thresholds["max_price"]),
                        True
                    ).otherwise(False)
                )
                
        # Volume validation
        for volume_field in ["volume_long", "size_long"]:
            if volume_field in df.columns:
                df_validated = df_validated.withColumn(
                    f"is_valid_{volume_field}",
                    when(
                        (col(volume_field).isNotNull()) &
                        (col(volume_field) >= self.quality_thresholds["min_volume"]) &
                        (col(volume_field) <= self.quality_thresholds["max_volume"]),
                        True
                    ).otherwise(False)
                )
                
        # Spread validation for quotes
        if "bid_price_decimal" in df.columns and "ask_price_decimal" in df.columns:
            df_validated = df_validated.withColumn(
                "spread_pct",
                when(
                    (col("bid_price_decimal").isNotNull()) & 
                    (col("ask_price_decimal").isNotNull()) &
                    (col("bid_price_decimal") > 0),
                    ((col("ask_price_decimal") - col("bid_price_decimal")) / col("bid_price_decimal")) * 100
                ).otherwise(None)
            ).withColumn(
                "is_valid_spread",
                when(
                    (col("spread_pct").isNotNull()) &
                    (col("spread_pct") >= 0) &
                    (col("spread_pct") <= self.quality_thresholds["max_spread_pct"]),
                    True
                ).otherwise(False)
            )
            
        # OHLC validation for bars
        if all(f"{field}_decimal" in df.columns for field in ["open", "high", "low", "close"]):
            df_validated = df_validated.withColumn(
                "is_valid_ohlc",
                when(
                    (col("high_decimal") >= col("low_decimal")) &
                    (col("high_decimal") >= col("open_decimal")) &
                    (col("high_decimal") >= col("close_decimal")) &
                    (col("low_decimal") <= col("open_decimal")) &
                    (col("low_decimal") <= col("close_decimal")),
                    True
                ).otherwise(False)
            )
            
        return df_validated
        
    def _deduplicate_records(self, df: DataFrame) -> DataFrame:
        """
        Remove duplicate records based on key fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Deduplicated DataFrame
        """
        # Define window for deduplication
        window_spec = Window.partitionBy("symbol_clean", "timestamp_parsed", "topic").orderBy(desc("ingestion_timestamp"))
        
        # Add row number to identify duplicates
        df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
        
        # Keep only the latest record for each key
        df_deduped = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
        
        return df_deduped
        
    def _handle_outliers(self, df: DataFrame) -> DataFrame:
        """
        Detect and handle statistical outliers.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with outlier flags and corrections
        """
        # Window for statistical calculations per symbol
        stats_window = Window.partitionBy("symbol_clean").rowsBetween(-100, 0)
        
        # Calculate rolling statistics for price fields
        price_fields = ["bid_price_decimal", "ask_price_decimal", "last_price_decimal", "price_decimal"]
        
        df_with_stats = df
        
        for field in price_fields:
            if field in df.columns:
                df_with_stats = df_with_stats.withColumn(
                    f"{field}_mean",
                    mean(col(field)).over(stats_window)
                ).withColumn(
                    f"{field}_stddev",
                    stddev(col(field)).over(stats_window)
                )
                
                # Flag outliers (>3 standard deviations)
                df_with_stats = df_with_stats.withColumn(
                    f"is_outlier_{field}",
                    when(
                        (col(f"{field}_stddev").isNotNull()) &
                        (col(f"{field}_stddev") > 0) &
                        (abs(col(field) - col(f"{field}_mean")) > (3 * col(f"{field}_stddev"))),
                        True
                    ).otherwise(False)
                )
                
        return df_with_stats
        
    def _enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Enrich data with additional computed fields and reference data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Enriched DataFrame
        """
        df_enriched = df
        
        # Add derived fields for quotes
        if "bid_price_decimal" in df.columns and "ask_price_decimal" in df.columns:
            df_enriched = df_enriched.withColumn(
                "mid_price",
                (col("bid_price_decimal") + col("ask_price_decimal")) / 2
            ).withColumn(
                "spread_bps",
                when(
                    (col("bid_price_decimal").isNotNull()) & 
                    (col("ask_price_decimal").isNotNull()) &
                    (col("bid_price_decimal") > 0),
                    ((col("ask_price_decimal") - col("bid_price_decimal")) / col("bid_price_decimal")) * 10000
                ).otherwise(None)
            )
            
        # Add derived fields for bars
        if all(f"{field}_decimal" in df.columns for field in ["open", "high", "low", "close"]):
            df_enriched = df_enriched.withColumn(
                "price_range",
                col("high_decimal") - col("low_decimal")
            ).withColumn(
                "price_change",
                col("close_decimal") - col("open_decimal")
            ).withColumn(
                "price_change_pct",
                when(
                    col("open_decimal") > 0,
                    ((col("close_decimal") - col("open_decimal")) / col("open_decimal")) * 100
                ).otherwise(None)
            )
            
        # Add time-based features
        df_enriched = df_enriched.withColumn(
            "hour_of_day",
            cast(col("timestamp_parsed"), "string").substr(12, 2).cast("int")
        ).withColumn(
            "day_of_week",
            cast(col("timestamp_parsed"), "string")  # Simplified - would use date functions
        ).withColumn(
            "is_market_hours",
            when(
                (col("hour_of_day") >= 9) & (col("hour_of_day") <= 16),
                True
            ).otherwise(False)
        )
        
        return df_enriched
        
    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        """
        Add Silver layer metadata and final transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: DataFrame with Silver metadata
        """
        from pyspark.sql.functions import current_timestamp, lit, uuid
        
        df_final = df.withColumn(
            "silver_processed_timestamp",
            current_timestamp()
        ).withColumn(
            "silver_job_id",
            lit(self.job_name)
        ).withColumn(
            "record_id",
            uuid()
        ).withColumn(
            "data_version",
            lit("1.0")
        )
        
        # Calculate overall quality score
        quality_columns = [col_name for col_name in df.columns if col_name.startswith("is_valid_")]
        if quality_columns:
            quality_sum = sum([col(c).cast("int") for c in quality_columns])
            df_final = df_final.withColumn(
                "overall_quality_score",
                quality_sum / len(quality_columns)
            ).withColumn(
                "is_silver_quality",
                col("overall_quality_score") >= 0.8
            )
            
        return df_final
        
    def get_sink_options(self) -> Dict[str, Any]:
        """Get Silver layer sink configuration."""
        silver_config = self.config.get("silver_layer", {})
        
        return {
            "path": silver_config.get("output_path", "/tmp/silver"),
            "format": "delta",
            "output_mode": "append",
            "trigger_interval": silver_config.get("trigger_interval", "1 minute"),
            "options": {
                "mergeSchema": "true",
                "autoOptimize": "true",
                "optimizeWrite": "true",
                "partitionBy": "symbol_clean,year,month,day",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.columnMapping.mode": "name"
            }
        }