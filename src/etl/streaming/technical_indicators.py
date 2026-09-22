"""
Technical indicators streaming job for real-time calculation of trading indicators.

This job computes various technical indicators from OHLCV data including:
- Moving averages (SMA, EMA)
- Bollinger Bands
- RSI, MACD, Stochastic
- Custom indicators
"""

from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lag, lead, avg, sum as spark_sum, count,
    min as spark_min, max as spark_max, stddev, sqrt, abs as spark_abs,
    row_number, rank, dense_rank, desc, asc, lit, current_timestamp,
    expr, coalesce, round as spark_round, isnan, isnull
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import structlog

from .base_streaming_job import BaseStreamingJob

logger = structlog.get_logger(__name__)


class TechnicalIndicatorsJob(BaseStreamingJob):
    """
    Technical indicators streaming job for real-time indicator calculation.
    
    Features:
    - Real-time moving averages (SMA, EMA)
    - Momentum indicators (RSI, Stochastic)
    - Trend indicators (MACD, Bollinger Bands)
    - Volume indicators
    - Custom indicator extensibility
    - Multi-timeframe support
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Technical Indicators job.
        
        Args:
            config: Job configuration including indicator parameters
        """
        super().__init__(config, "technical-indicators")
        self.gold_path = config.get("gold_layer", {}).get("output_path", "/tmp/gold")
        
        # Indicator configuration
        self.indicators_config = config.get("technical_indicators", {})
        
        # Supported indicators and their parameters
        self.moving_averages = self.indicators_config.get("moving_averages", {
            "periods": [5, 10, 20, 50, 100, 200],
            "types": ["sma", "ema"]
        })
        
        self.bollinger_bands = self.indicators_config.get("bollinger_bands", {
            "period": 20,
            "std_dev": 2
        })
        
        self.rsi_config = self.indicators_config.get("rsi", {"period": 14})
        self.macd_config = self.indicators_config.get("macd", {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9
        })
        self.stochastic_config = self.indicators_config.get("stochastic", {
            "k_period": 14,
            "d_period": 3
        })
        
    def create_source_stream(self) -> DataFrame:
        """
        Create source stream from Gold layer OHLCV data.
        
        Returns:
            DataFrame: Gold layer OHLCV streaming DataFrame
        """
        try:
            self.logger.info("Creating source stream from Gold layer OHLCV data", 
                           gold_path=self.gold_path)
            
            # Read OHLCV bars from Gold layer
            df = (
                self.spark
                .readStream
                .format("delta")
                .option("ignoreChanges", "true")
                .option("ignoreDeletes", "true")
                .load(self.gold_path)
                .filter(col("aggregation_type") == "ohlcv_bars")  # Only OHLCV data
                .filter(col("timeframe").isin(["1 minute", "5 minutes", "15 minutes", "1 hour"]))  # Focus on key timeframes
                .withWatermark("window_start", "10 minutes")
            )
            
            return df
            
        except Exception as e:
            self.logger.error("Failed to create Gold layer source stream", error=str(e))
            raise
            
    def transform_data(self, df: DataFrame) -> DataFrame:
        """
        Calculate technical indicators.
        
        Args:
            df: OHLCV DataFrame
            
        Returns:
            DataFrame: DataFrame with technical indicators
        """
        try:
            self.logger.info("Starting technical indicators calculation")
            
            # Process each timeframe separately
            timeframes = df.select("timeframe").distinct().rdd.map(lambda row: row[0]).collect()
            
            indicator_dfs = []
            
            for timeframe in timeframes:
                timeframe_df = df.filter(col("timeframe") == timeframe)
                
                # Calculate indicators for this timeframe
                indicators_df = self._calculate_indicators_for_timeframe(timeframe_df, timeframe)
                indicator_dfs.append(indicators_df)
                
            # Union all timeframes
            if indicator_dfs:
                result_df = indicator_dfs[0]
                for i in range(1, len(indicator_dfs)):
                    result_df = result_df.unionByName(indicator_dfs[i])
                    
                # Add metadata
                result_df = result_df.withColumn(
                    "indicators_processed_timestamp",
                    current_timestamp()
                ).withColumn(
                    "indicators_job_id",
                    lit(self.job_name)
                ).withColumn(
                    "indicator_version",
                    lit("1.0")
                )
                
                self.logger.info("Technical indicators calculation completed", 
                               timeframes_processed=len(timeframes))
                return result_df
            else:
                raise ValueError("No data to process for technical indicators")
                
        except Exception as e:
            self.logger.error("Failed to calculate technical indicators", error=str(e))
            raise
            
    def _calculate_indicators_for_timeframe(self, df: DataFrame, timeframe: str) -> DataFrame:
        """
        Calculate all technical indicators for a specific timeframe.
        
        Args:
            df: OHLCV DataFrame for specific timeframe
            timeframe: Timeframe string
            
        Returns:
            DataFrame: DataFrame with calculated indicators
        """
        # Define window specifications for calculations
        # Order by symbol and time for proper indicator calculation
        window_spec = (Window
                      .partitionBy("symbol")
                      .orderBy("window_start")
                      .rangeBetween(Window.unboundedPreceding, Window.currentRow))
        
        # Start with base OHLCV data
        indicators_df = df
        
        # 1. Moving Averages
        indicators_df = self._calculate_moving_averages(indicators_df, window_spec)
        
        # 2. Bollinger Bands
        indicators_df = self._calculate_bollinger_bands(indicators_df, window_spec)
        
        # 3. RSI
        indicators_df = self._calculate_rsi(indicators_df, window_spec)
        
        # 4. MACD
        indicators_df = self._calculate_macd(indicators_df, window_spec)
        
        # 5. Stochastic
        indicators_df = self._calculate_stochastic(indicators_df, window_spec)
        
        # 6. Volume indicators
        indicators_df = self._calculate_volume_indicators(indicators_df, window_spec)
        
        # 7. Price action indicators
        indicators_df = self._calculate_price_action_indicators(indicators_df, window_spec)
        
        return indicators_df
        
    def _calculate_moving_averages(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate Simple and Exponential Moving Averages."""
        result_df = df
        
        periods = self.moving_averages["periods"]
        types = self.moving_averages["types"]
        
        for period in periods:
            # Window for the specific period
            period_window = (Window
                           .partitionBy("symbol")
                           .orderBy("window_start")
                           .rowsBetween(-period + 1, 0))
            
            if "sma" in types:
                # Simple Moving Average
                result_df = result_df.withColumn(
                    f"sma_{period}",
                    avg("close_price").over(period_window)
                )
                
            if "ema" in types:
                # Exponential Moving Average (simplified calculation)
                # In production, you'd implement proper EMA with alpha = 2/(period+1)
                alpha = 2.0 / (period + 1)
                
                # For streaming, we'll use a recursive calculation approximation
                # This is simplified - proper EMA requires maintaining state
                result_df = result_df.withColumn(
                    f"ema_{period}_temp",
                    avg("close_price").over(period_window)
                ).withColumn(
                    f"ema_{period}",
                    col(f"ema_{period}_temp")  # Simplified - would need proper EMA calculation
                ).drop(f"ema_{period}_temp")
                
        return result_df
        
    def _calculate_bollinger_bands(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate Bollinger Bands."""
        period = self.bollinger_bands["period"]
        std_dev_multiplier = self.bollinger_bands["std_dev"]
        
        # Window for Bollinger Bands
        bb_window = (Window
                    .partitionBy("symbol")
                    .orderBy("window_start")
                    .rowsBetween(-period + 1, 0))
        
        result_df = df.withColumn(
            "bb_middle",
            avg("close_price").over(bb_window)
        ).withColumn(
            "bb_std",
            stddev("close_price").over(bb_window)
        ).withColumn(
            "bb_upper",
            col("bb_middle") + (col("bb_std") * std_dev_multiplier)
        ).withColumn(
            "bb_lower",
            col("bb_middle") - (col("bb_std") * std_dev_multiplier)
        ).withColumn(
            "bb_width",
            col("bb_upper") - col("bb_lower")
        ).withColumn(
            "bb_position",
            when(col("bb_width") > 0, 
                 (col("close_price") - col("bb_lower")) / col("bb_width")
            ).otherwise(0.5)
        ).drop("bb_std")
        
        return result_df
        
    def _calculate_rsi(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate Relative Strength Index (RSI)."""
        period = self.rsi_config["period"]
        
        # Calculate price changes
        price_change_window = (Window
                             .partitionBy("symbol")
                             .orderBy("window_start"))
        
        rsi_window = (Window
                     .partitionBy("symbol")
                     .orderBy("window_start")
                     .rowsBetween(-period + 1, 0))
        
        result_df = df.withColumn(
            "price_change",
            col("close_price") - lag("close_price", 1).over(price_change_window)
        ).withColumn(
            "gain",
            when(col("price_change") > 0, col("price_change")).otherwise(0)
        ).withColumn(
            "loss",
            when(col("price_change") < 0, -col("price_change")).otherwise(0)
        ).withColumn(
            "avg_gain",
            avg("gain").over(rsi_window)
        ).withColumn(
            "avg_loss",
            avg("loss").over(rsi_window)
        ).withColumn(
            "rs",
            when(col("avg_loss") > 0, col("avg_gain") / col("avg_loss")).otherwise(100)
        ).withColumn(
            "rsi",
            100 - (100 / (1 + col("rs")))
        ).drop("price_change", "gain", "loss", "avg_gain", "avg_loss", "rs")
        
        return result_df
        
    def _calculate_macd(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate MACD (Moving Average Convergence Divergence)."""
        fast_period = self.macd_config["fast_period"]
        slow_period = self.macd_config["slow_period"]
        signal_period = self.macd_config["signal_period"]
        
        # Calculate EMAs for MACD (simplified)
        fast_window = (Window
                      .partitionBy("symbol")
                      .orderBy("window_start")
                      .rowsBetween(-fast_period + 1, 0))
        
        slow_window = (Window
                      .partitionBy("symbol")
                      .orderBy("window_start")
                      .rowsBetween(-slow_period + 1, 0))
        
        signal_window = (Window
                        .partitionBy("symbol")
                        .orderBy("window_start")
                        .rowsBetween(-signal_period + 1, 0))
        
        result_df = df.withColumn(
            "ema_fast",
            avg("close_price").over(fast_window)  # Simplified EMA
        ).withColumn(
            "ema_slow",
            avg("close_price").over(slow_window)  # Simplified EMA
        ).withColumn(
            "macd_line",
            col("ema_fast") - col("ema_slow")
        ).withColumn(
            "macd_signal",
            avg("macd_line").over(signal_window)  # Simplified signal line
        ).withColumn(
            "macd_histogram",
            col("macd_line") - col("macd_signal")
        ).drop("ema_fast", "ema_slow")
        
        return result_df
        
    def _calculate_stochastic(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate Stochastic Oscillator."""
        k_period = self.stochastic_config["k_period"]
        d_period = self.stochastic_config["d_period"]
        
        # %K calculation window
        k_window = (Window
                   .partitionBy("symbol")
                   .orderBy("window_start")
                   .rowsBetween(-k_period + 1, 0))
        
        # %D calculation window
        d_window = (Window
                   .partitionBy("symbol")
                   .orderBy("window_start")
                   .rowsBetween(-d_period + 1, 0))
        
        result_df = df.withColumn(
            "lowest_low",
            spark_min("low_price").over(k_window)
        ).withColumn(
            "highest_high",
            spark_max("high_price").over(k_window)
        ).withColumn(
            "stoch_k",
            when(
                (col("highest_high") - col("lowest_low")) > 0,
                ((col("close_price") - col("lowest_low")) / 
                 (col("highest_high") - col("lowest_low"))) * 100
            ).otherwise(50)
        ).withColumn(
            "stoch_d",
            avg("stoch_k").over(d_window)
        ).drop("lowest_low", "highest_high")
        
        return result_df
        
    def _calculate_volume_indicators(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate volume-based indicators."""
        # Volume moving averages
        volume_window_20 = (Window
                           .partitionBy("symbol")
                           .orderBy("window_start")
                           .rowsBetween(-19, 0))
        
        result_df = df.withColumn(
            "volume_sma_20",
            avg("volume").over(volume_window_20)
        ).withColumn(
            "volume_ratio",
            when(col("volume_sma_20") > 0, 
                 col("volume") / col("volume_sma_20")
            ).otherwise(1.0)
        ).withColumn(
            "price_volume",
            col("close_price") * col("volume")
        ).withColumn(
            "vwap",
            when(col("volume") > 0, col("price_volume") / col("volume")).otherwise(col("close_price"))
        )
        
        # On Balance Volume (OBV) - simplified
        obv_window = (Window
                     .partitionBy("symbol")
                     .orderBy("window_start")
                     .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        
        result_df = result_df.withColumn(
            "price_change_obv",
            col("close_price") - lag("close_price", 1).over(obv_window)
        ).withColumn(
            "obv_change",
            when(col("price_change_obv") > 0, col("volume"))
            .when(col("price_change_obv") < 0, -col("volume"))
            .otherwise(0)
        ).withColumn(
            "obv",
            spark_sum("obv_change").over(obv_window)
        ).drop("price_change_obv", "obv_change", "price_volume")
        
        return result_df
        
    def _calculate_price_action_indicators(self, df: DataFrame, window_spec: Window) -> DataFrame:
        """Calculate price action indicators."""
        # Average True Range (ATR)
        atr_period = 14
        atr_window = (Window
                     .partitionBy("symbol")
                     .orderBy("window_start")
                     .rowsBetween(-atr_period + 1, 0))
        
        result_df = df.withColumn(
            "prev_close",
            lag("close_price", 1).over(window_spec)
        ).withColumn(
            "true_range",
            spark_max(
                col("high_price") - col("low_price"),
                spark_abs(col("high_price") - coalesce(col("prev_close"), col("close_price"))),
                spark_abs(col("low_price") - coalesce(col("prev_close"), col("close_price")))
            )
        ).withColumn(
            "atr",
            avg("true_range").over(atr_window)
        )
        
        # Pivot points (simplified daily calculation)
        result_df = result_df.withColumn(
            "pivot_point",
            (col("high_price") + col("low_price") + col("close_price")) / 3
        ).withColumn(
            "resistance_1",
            (2 * col("pivot_point")) - col("low_price")
        ).withColumn(
            "support_1", 
            (2 * col("pivot_point")) - col("high_price")
        )
        
        # Price position indicators
        result_df = result_df.withColumn(
            "price_position_range",
            when(
                (col("high_price") - col("low_price")) > 0,
                (col("close_price") - col("low_price")) / (col("high_price") - col("low_price"))
            ).otherwise(0.5)
        ).withColumn(
            "body_size",
            spark_abs(col("close_price") - col("open_price"))
        ).withColumn(
            "upper_shadow",
            col("high_price") - spark_max(col("open_price"), col("close_price"))
        ).withColumn(
            "lower_shadow",
            spark_min(col("open_price"), col("close_price")) - col("low_price")
        ).drop("prev_close", "true_range")
        
        return result_df
        
    def get_sink_options(self) -> Dict[str, Any]:
        """Get Technical Indicators sink configuration."""
        indicators_config = self.config.get("technical_indicators", {})
        
        return {
            "path": indicators_config.get("output_path", "/tmp/indicators"),
            "format": "delta",
            "output_mode": "append",
            "trigger_interval": indicators_config.get("trigger_interval", "3 minutes"),
            "options": {
                "mergeSchema": "true",
                "autoOptimize": "true",
                "optimizeWrite": "true",
                "partitionBy": "symbol,timeframe,year,month,day",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.columnMapping.mode": "name",
                "delta.enableChangeDataFeed": "true"
            }
        }