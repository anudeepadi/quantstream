"""
Base streaming job class providing common functionality for all streaming ETL jobs.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType
from delta import configure_spark_with_delta_pip
import structlog

logger = structlog.get_logger(__name__)


class BaseStreamingJob(ABC):
    """
    Abstract base class for all streaming ETL jobs.
    
    Provides common functionality for Spark streaming jobs including:
    - Spark session management
    - Configuration handling
    - Error handling and monitoring
    - Checkpoint management
    """
    
    def __init__(self, config: Dict[str, Any], job_name: str):
        """
        Initialize the streaming job.
        
        Args:
            config: Job configuration dictionary
            job_name: Name of the streaming job
        """
        self.config = config
        self.job_name = job_name
        self.logger = logger.bind(job_name=job_name)
        self.spark = self._create_spark_session()
        self.checkpoint_location = self._get_checkpoint_location()
        
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session with Delta Lake support."""
        builder = (
            SparkSession.builder
            .appName(f"QuantStream-{self.job_name}")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.streaming.checkpointLocation.deleteOnStop", "false")
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        )
        
        # Add streaming specific configurations
        streaming_config = self.config.get("streaming", {})
        for key, value in streaming_config.items():
            builder = builder.config(f"spark.sql.streaming.{key}", str(value))
            
        # Add performance optimizations
        perf_config = self.config.get("performance", {})
        if perf_config.get("enable_aqe", True):
            builder = (builder
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            )
            
        if perf_config.get("enable_photon", False):
            builder = builder.config("spark.databricks.photon.enabled", "true")
            
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        self.logger.info("Spark session created", 
                        app_name=spark.conf.get("spark.app.name"),
                        spark_version=spark.version)
        
        return spark
        
    def _get_checkpoint_location(self) -> str:
        """Get checkpoint location for streaming query."""
        base_path = self.config.get("checkpoint_base_path", "/tmp/checkpoints")
        return f"{base_path}/{self.job_name}"
        
    @abstractmethod
    def create_source_stream(self) -> DataFrame:
        """Create the source streaming DataFrame."""
        pass
        
    @abstractmethod
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply transformations to the streaming data."""
        pass
        
    @abstractmethod
    def get_sink_options(self) -> Dict[str, Any]:
        """Get sink configuration options."""
        pass
        
    def run(self) -> StreamingQuery:
        """
        Run the streaming job.
        
        Returns:
            StreamingQuery: The active streaming query
        """
        try:
            self.logger.info("Starting streaming job")
            
            # Create source stream
            source_df = self.create_source_stream()
            
            # Apply transformations
            transformed_df = self.transform_data(source_df)
            
            # Configure sink
            sink_options = self.get_sink_options()
            
            # Start streaming query
            query = (
                transformed_df.writeStream
                .trigger(processingTime=sink_options.get("trigger_interval", "10 seconds"))
                .option("checkpointLocation", self.checkpoint_location)
                .outputMode(sink_options.get("output_mode", "append"))
                .format(sink_options.get("format", "delta"))
                .option("path", sink_options["path"])
            )
            
            # Add additional options
            for key, value in sink_options.get("options", {}).items():
                query = query.option(key, value)
                
            streaming_query = query.start()
            
            self.logger.info("Streaming job started", 
                           query_id=streaming_query.id,
                           checkpoint_location=self.checkpoint_location)
            
            return streaming_query
            
        except Exception as e:
            self.logger.error("Failed to start streaming job", error=str(e))
            raise
            
    def stop(self, query: StreamingQuery, timeout: Optional[int] = None) -> None:
        """
        Stop the streaming query gracefully.
        
        Args:
            query: The streaming query to stop
            timeout: Timeout in seconds for stopping the query
        """
        try:
            if query.isActive:
                self.logger.info("Stopping streaming job", query_id=query.id)
                if timeout:
                    query.stop()
                    query.awaitTermination(timeout)
                else:
                    query.stop()
                self.logger.info("Streaming job stopped successfully")
            else:
                self.logger.info("Streaming job was already stopped")
                
        except Exception as e:
            self.logger.error("Error stopping streaming job", error=str(e))
            raise
            
    def get_query_status(self, query: StreamingQuery) -> Dict[str, Any]:
        """
        Get current status of the streaming query.
        
        Args:
            query: The streaming query
            
        Returns:
            Dictionary containing query status information
        """
        try:
            progress = query.lastProgress
            status = {
                "query_id": query.id,
                "is_active": query.isActive,
                "name": query.name,
                "checkpoint_location": self.checkpoint_location,
                "last_progress": progress
            }
            
            if progress:
                status.update({
                    "batch_id": progress.get("batchId"),
                    "input_rows_per_second": progress.get("inputRowsPerSecond"),
                    "processed_rows_per_second": progress.get("processedRowsPerSecond"),
                    "batch_duration_ms": progress.get("batchDuration"),
                    "num_input_rows": progress.get("numInputRows"),
                })
                
            return status
            
        except Exception as e:
            self.logger.error("Error getting query status", error=str(e))
            return {"error": str(e)}
            
    def cleanup(self) -> None:
        """Clean up resources."""
        try:
            if self.spark:
                self.spark.stop()
                self.logger.info("Spark session stopped")
        except Exception as e:
            self.logger.error("Error during cleanup", error=str(e))
            
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.cleanup()