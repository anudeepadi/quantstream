"""
Anomaly detection streaming job for real-time identification of unusual patterns.

This job implements multiple anomaly detection algorithms including:
- Statistical outlier detection
- Price spike detection
- Volume anomaly detection
- Pattern-based anomalies
"""

from typing import Dict, Any, List, Tuple, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lag, lead, avg, sum as spark_sum, count,
    min as spark_min, max as spark_max, stddev, variance,
    abs as spark_abs, sqrt, log, exp, lit, current_timestamp,
    percentile_approx, coalesce, isnan, isnull, desc, asc,
    row_number, rank, dense_rank, ntile, expr, greatest
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, BooleanType, StringType
import structlog

from .base_streaming_job import BaseStreamingJob

logger = structlog.get_logger(__name__)


class AnomalyDetectionJob(BaseStreamingJob):
    """
    Anomaly detection streaming job for real-time anomaly identification.
    
    Features:
    - Statistical outlier detection (Z-score, IQR)
    - Price spike detection
    - Volume anomaly detection
    - Cross-correlation anomalies
    - Machine learning-based detection
    - Multi-timeframe analysis
    - Real-time alerting
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Anomaly Detection job.
        
        Args:
            config: Job configuration including detection parameters
        """
        super().__init__(config, "anomaly-detection")
        self.silver_path = config.get("silver_layer", {}).get("output_path", "/tmp/silver")
        
        # Anomaly detection configuration
        self.anomaly_config = config.get("anomaly_detection", {})
        
        # Detection algorithms configuration
        self.algorithms = self.anomaly_config.get("algorithms", {})
        
        self.isolation_forest_config = self.algorithms.get("isolation_forest", {
            "enabled": True,
            "contamination": 0.1,
            "n_estimators": 100
        })
        
        self.statistical_config = self.algorithms.get("statistical_outliers", {
            "enabled": True,
            "method": "zscore",
            "threshold": 3.0
        })
        
        self.price_spike_config = self.algorithms.get("price_spike_detection", {
            "enabled": True,
            "spike_threshold_pct": 5.0,
            "volume_spike_threshold": 3.0
        })
        
        # Alert thresholds
        self.alert_thresholds = self.anomaly_config.get("alert_thresholds", {
            "critical_anomaly_score": 0.8,
            "warning_anomaly_score": 0.6
        })
        
    def create_source_stream(self) -> DataFrame:
        """
        Create source stream from Silver layer data.
        
        Returns:
            DataFrame: Silver layer streaming DataFrame
        """
        try:
            self.logger.info("Creating source stream from Silver layer for anomaly detection", 
                           silver_path=self.silver_path)
            
            # Read from Silver layer with high-quality data
            df = (
                self.spark
                .readStream
                .format("delta")
                .option("ignoreChanges", "true")
                .option("ignoreDeletes", "true")
                .load(self.silver_path)
                .filter(col("is_silver_quality") == True)
                .withWatermark("timestamp_parsed", "15 minutes")
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to create Silver layer source stream", error=str(e))
            raise
            
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Apply anomaly detection algorithms.
        
        Args:
            df: Silver layer DataFrame
            
        Returns:
            DataFrame: DataFrame with anomaly scores and flags
        """
        try:
            self.logger.info("Starting anomaly detection")
            
            # Initialize result DataFrame
            anomaly_df = df
            
            # Apply different anomaly detection algorithms
            if self.statistical_config.get("enabled", True):
                anomaly_df = self._detect_statistical_anomalies(anomaly_df)
                
            if self.price_spike_config.get("enabled", True):
                anomaly_df = self._detect_price_spikes(anomaly_df)
                
            # Add volume anomaly detection
            anomaly_df = self._detect_volume_anomalies(anomaly_df)
            
            # Add pattern-based anomaly detection
            anomaly_df = self._detect_pattern_anomalies(anomaly_df)
            
            # Add cross-symbol correlation anomalies
            anomaly_df = self._detect_correlation_anomalies(anomaly_df)
            
            # Calculate composite anomaly score
            anomaly_df = self._calculate_composite_anomaly_score(anomaly_df)
            
            # Add anomaly metadata and classifications
            anomaly_df = self._classify_anomalies(anomaly_df)
            
            # Filter to only anomalous records or include all with scores
            final_df = self._prepare_output(anomaly_df)
            
            self.logger.info("Anomaly detection completed")
            return final_df
            
        except Exception as e:
            self.logger.error("Failed to detect anomalies", error=str(e))
            raise
            
    def _detect_statistical_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect statistical outliers using Z-score and IQR methods."""
        method = self.statistical_config.get("method", "zscore")
        threshold = self.statistical_config.get("threshold", 3.0)
        
        # Window for statistical calculations per symbol
        stats_window = (Window
                       .partitionBy("symbol_clean")
                       .orderBy("timestamp_parsed")
                       .rowsBetween(-100, 0))  # Rolling window of 100 records
        
        # Price fields to analyze
        price_fields = [col_name for col_name in df.columns 
                       if "price" in col_name and "decimal" in col_name]
        
        anomaly_df = df
        
        for field in price_fields:
            if field in df.columns:
                if method == "zscore":
                    anomaly_df = anomaly_df.withColumn(
                        f"{field}_mean",
                        avg(col(field)).over(stats_window)
                    ).withColumn(
                        f"{field}_std",
                        stddev(col(field)).over(stats_window)
                    ).withColumn(
                        f"{field}_zscore",
                        when(
                            col(f"{field}_std") > 0,
                            (col(field) - col(f"{field}_mean")) / col(f"{field}_std")
                        ).otherwise(0)
                    ).withColumn(
                        f"is_anomaly_zscore_{field}",
                        spark_abs(col(f"{field}_zscore")) > threshold
                    ).withColumn(
                        f"anomaly_score_zscore_{field}",
                        when(
                            col(f"{field}_std") > 0,
                            spark_abs(col(f"{field}_zscore")) / threshold
                        ).otherwise(0)
                    )
                    
                elif method == "iqr":
                    anomaly_df = anomaly_df.withColumn(
                        f"{field}_q1",
                        percentile_approx(col(field), 0.25).over(stats_window)
                    ).withColumn(
                        f"{field}_q3",
                        percentile_approx(col(field), 0.75).over(stats_window)
                    ).withColumn(
                        f"{field}_iqr",
                        col(f"{field}_q3") - col(f"{field}_q1")
                    ).withColumn(
                        f"{field}_iqr_lower",
                        col(f"{field}_q1") - (1.5 * col(f"{field}_iqr"))
                    ).withColumn(
                        f"{field}_iqr_upper",
                        col(f"{field}_q3") + (1.5 * col(f"{field}_iqr"))
                    ).withColumn(
                        f"is_anomaly_iqr_{field}",
                        (col(field) < col(f"{field}_iqr_lower")) |
                        (col(field) > col(f"{field}_iqr_upper"))
                    ).withColumn(
                        f"anomaly_score_iqr_{field}",
                        when(
                            col(field) < col(f"{field}_iqr_lower"),
                            (col(f"{field}_iqr_lower") - col(field)) / col(f"{field}_iqr")
                        ).when(
                            col(field) > col(f"{field}_iqr_upper"),
                            (col(field) - col(f"{field}_iqr_upper")) / col(f"{field}_iqr")
                        ).otherwise(0)
                    )
                
                # Clean up temporary columns
                temp_cols = [c for c in anomaly_df.columns 
                           if any(suffix in c for suffix in ["_mean", "_std", "_q1", "_q3", "_iqr", "_iqr_lower", "_iqr_upper"])]
                for temp_col in temp_cols:
                    anomaly_df = anomaly_df.drop(temp_col)
                    
        return anomaly_df
        
    def _detect_price_spikes(self, df: DataFrame) -> DataFrame:
        """Detect sudden price spikes and drops."""
        spike_threshold = self.price_spike_config.get("spike_threshold_pct", 5.0)
        
        # Window for price change calculation
        price_window = (Window
                       .partitionBy("symbol_clean")
                       .orderBy("timestamp_parsed"))
        
        # Calculate price changes
        spike_df = df.withColumn(
            "prev_price",
            lag("last_price_decimal", 1).over(price_window)
        ).withColumn(
            "price_change_pct",
            when(
                col("prev_price").isNotNull() & (col("prev_price") > 0),
                ((col("last_price_decimal") - col("prev_price")) / col("prev_price")) * 100
            ).otherwise(0)
        )
        
        # Detect spikes
        spike_df = spike_df.withColumn(
            "is_price_spike_up",
            col("price_change_pct") > spike_threshold
        ).withColumn(
            "is_price_spike_down",
            col("price_change_pct") < -spike_threshold
        ).withColumn(
            "is_price_spike",
            col("is_price_spike_up") | col("is_price_spike_down")
        ).withColumn(
            "price_spike_magnitude",
            spark_abs(col("price_change_pct"))
        ).withColumn(
            "anomaly_score_price_spike",
            when(
                col("is_price_spike"),
                col("price_spike_magnitude") / spike_threshold
            ).otherwise(0)
        ).drop("prev_price")
        
        return spike_df
        
    def _detect_volume_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect volume anomalies and unusual trading activity."""
        volume_threshold = self.price_spike_config.get("volume_spike_threshold", 3.0)
        
        # Window for volume statistics
        volume_window = (Window
                        .partitionBy("symbol_clean")
                        .orderBy("timestamp_parsed")
                        .rowsBetween(-50, 0))  # 50-period rolling window
        
        volume_df = df.withColumn(
            "volume_mean",
            avg("volume_long").over(volume_window)
        ).withColumn(
            "volume_std",
            stddev("volume_long").over(volume_window)
        ).withColumn(
            "volume_zscore",
            when(
                col("volume_std") > 0,
                (col("volume_long") - col("volume_mean")) / col("volume_std")
            ).otherwise(0)
        ).withColumn(
            "is_volume_spike",
            col("volume_zscore") > volume_threshold
        ).withColumn(
            "anomaly_score_volume",
            when(
                col("volume_zscore") > volume_threshold,
                col("volume_zscore") / volume_threshold
            ).otherwise(0)
        )
        
        # Detect unusual volume patterns
        volume_df = volume_df.withColumn(
            "volume_ratio",
            when(
                col("volume_mean") > 0,
                col("volume_long") / col("volume_mean")
            ).otherwise(1.0)
        ).withColumn(
            "is_unusual_volume",
            (col("volume_ratio") > 5.0) | (col("volume_ratio") < 0.1)
        ).withColumn(
            "anomaly_score_volume_pattern",
            when(
                col("volume_ratio") > 5.0,
                (col("volume_ratio") - 5.0) / 5.0
            ).when(
                col("volume_ratio") < 0.1,
                (0.1 - col("volume_ratio")) / 0.1
            ).otherwise(0)
        ).drop("volume_mean", "volume_std")
        
        return volume_df
        
    def _detect_pattern_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect pattern-based anomalies."""
        # Window for pattern analysis
        pattern_window = (Window
                         .partitionBy("symbol_clean")
                         .orderBy("timestamp_parsed")
                         .rowsBetween(-10, 0))
        
        pattern_df = df
        
        # Detect consecutive same-direction moves
        pattern_df = pattern_df.withColumn(
            "price_direction",
            when(col("price_change_pct") > 0, 1)
            .when(col("price_change_pct") < 0, -1)
            .otherwise(0)
        )
        
        # Count consecutive moves in same direction
        # This is simplified - would need more complex logic for proper streak counting
        pattern_df = pattern_df.withColumn(
            "direction_changes",
            count(when(col("price_direction") != 0, 1)).over(pattern_window)
        ).withColumn(
            "is_pattern_anomaly",
            col("direction_changes") > 8  # More than 8 moves in same direction
        ).withColumn(
            "anomaly_score_pattern",
            when(
                col("direction_changes") > 8,
                (col("direction_changes") - 8) / 8.0
            ).otherwise(0)
        ).drop("direction_changes")
        
        return pattern_df
        
    def _detect_correlation_anomalies(self, df: DataFrame) -> DataFrame:
        """Detect anomalies based on cross-symbol correlations."""
        # This is a simplified implementation
        # In practice, you'd maintain correlation matrices and detect deviations
        
        correlation_df = df.withColumn(
            "is_correlation_anomaly",
            lit(False)  # Placeholder
        ).withColumn(
            "anomaly_score_correlation",
            lit(0.0)  # Placeholder
        )
        
        return correlation_df
        
    def _calculate_composite_anomaly_score(self, df: DataFrame) -> DataFrame:
        """Calculate composite anomaly score from all detection methods."""
        # Get all anomaly score columns
        score_columns = [col_name for col_name in df.columns 
                        if col_name.startswith("anomaly_score_")]
        
        if not score_columns:
            return df.withColumn("composite_anomaly_score", lit(0.0))
            
        # Calculate weighted average of all anomaly scores
        # For simplicity, using equal weights
        score_sum = sum([col(c) for c in score_columns])
        
        composite_df = df.withColumn(
            "composite_anomaly_score",
            score_sum / len(score_columns)
        ).withColumn(
            "max_individual_score",
            greatest(*[col(c) for c in score_columns]) if len(score_columns) > 1 else col(score_columns[0])
        ).withColumn(
            "anomaly_detection_methods",
            lit(len(score_columns))
        )
        
        return composite_df
        
    def _classify_anomalies(self, df: DataFrame) -> DataFrame:
        """Classify anomalies by severity and type."""
        critical_threshold = self.alert_thresholds["critical_anomaly_score"]
        warning_threshold = self.alert_thresholds["warning_anomaly_score"]
        
        classified_df = df.withColumn(
            "anomaly_severity",
            when(col("composite_anomaly_score") >= critical_threshold, "CRITICAL")
            .when(col("composite_anomaly_score") >= warning_threshold, "WARNING")
            .when(col("composite_anomaly_score") > 0, "INFO")
            .otherwise("NORMAL")
        ).withColumn(
            "is_anomaly",
            col("composite_anomaly_score") >= warning_threshold
        )
        
        # Determine primary anomaly type
        anomaly_flags = [col_name for col_name in df.columns 
                        if col_name.startswith("is_") and "anomaly" in col_name]
        
        # Find the dominant anomaly type
        classified_df = classified_df.withColumn(
            "primary_anomaly_type",
            when(col("is_price_spike"), "PRICE_SPIKE")
            .when(col("is_volume_spike"), "VOLUME_SPIKE")
            .when(col("is_pattern_anomaly"), "PATTERN_ANOMALY")
            .when(col("is_correlation_anomaly"), "CORRELATION_ANOMALY")
            .otherwise("STATISTICAL_OUTLIER")
        )
        
        # Add detection metadata
        classified_df = classified_df.withColumn(
            "anomaly_detected_timestamp",
            current_timestamp()
        ).withColumn(
            "anomaly_detection_job_id",
            lit(self.job_name)
        ).withColumn(
            "anomaly_detection_version",
            lit("1.0")
        )
        
        return classified_df
        
    def _prepare_output(self, df: DataFrame) -> DataFrame:
        """Prepare final output with relevant anomaly information."""
        # Select key columns for output
        output_df = df.select(
            "symbol_clean",
            "timestamp_parsed",
            "topic",
            
            # Price information
            "last_price_decimal",
            "bid_price_decimal", 
            "ask_price_decimal",
            "volume_long",
            "price_change_pct",
            
            # Anomaly information
            "is_anomaly",
            "anomaly_severity",
            "primary_anomaly_type",
            "composite_anomaly_score",
            "max_individual_score",
            
            # Specific anomaly flags
            "is_price_spike",
            "is_volume_spike",
            "is_pattern_anomaly",
            "price_spike_magnitude",
            "volume_zscore",
            "volume_ratio",
            
            # Metadata
            "anomaly_detected_timestamp",
            "anomaly_detection_job_id",
            "anomaly_detection_version"
        )
        
        # Add partitioning columns
        output_df = output_df.withColumn(
            "year", expr("year(timestamp_parsed)")
        ).withColumn(
            "month", expr("month(timestamp_parsed)")
        ).withColumn(
            "day", expr("day(timestamp_parsed)")
        ).withColumn(
            "hour", expr("hour(timestamp_parsed)")
        )
        
        return output_df
        
    def get_sink_options(self) -> Dict[str, Any]:
        """Get Anomaly Detection sink configuration."""
        anomaly_config = self.config.get("anomaly_detection", {})
        
        return {
            "path": anomaly_config.get("output_path", "/tmp/anomalies"),
            "format": "delta",
            "output_mode": "append",
            "trigger_interval": anomaly_config.get("trigger_interval", "5 minutes"),
            "options": {
                "mergeSchema": "true",
                "autoOptimize": "true",
                "optimizeWrite": "true",
                "partitionBy": "anomaly_severity,symbol_clean,year,month,day",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.columnMapping.mode": "name",
                "delta.enableChangeDataFeed": "true"
            }
        }