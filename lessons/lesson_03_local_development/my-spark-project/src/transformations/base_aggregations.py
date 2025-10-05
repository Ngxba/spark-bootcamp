"""
Base data aggregation transformations for Spark DataFrames.
This module provides generic, reusable aggregation functions that can be applied across domains.
"""

from typing import Dict, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg,
    count,
    max as spark_max,
    min as spark_min,
    countDistinct,
    desc,
    asc,
    round as spark_round,
    date_format,
    year,
    quarter,
    stddev,
    lit,
    concat,
)
from pyspark.sql.window import Window


class BaseDataAggregator:
    """
    Base data aggregation utility class for creating generic aggregations.

    This class provides methods for:
    - Basic statistical aggregations
    - Time-based grouping
    - Window functions
    - Ranking and percentiles
    """

    @staticmethod
    def basic_stats_by_group(
        df: DataFrame, group_cols: List[str], numeric_cols: List[str]
    ) -> DataFrame:
        """
        Calculate basic statistics for numeric columns grouped by specified columns.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            numeric_cols: Numeric columns to aggregate

        Returns:
            DataFrame with basic statistics
        """
        agg_exprs = []

        for col_name in numeric_cols:
            agg_exprs.extend(
                [
                    count(col(col_name)).alias(f"{col_name}_count"),
                    spark_sum(col(col_name)).alias(f"{col_name}_sum"),
                    avg(col(col_name)).alias(f"{col_name}_avg"),
                    spark_min(col(col_name)).alias(f"{col_name}_min"),
                    spark_max(col(col_name)).alias(f"{col_name}_max"),
                    stddev(col(col_name)).alias(f"{col_name}_stddev"),
                ]
            )

        return df.groupBy(*group_cols).agg(*agg_exprs)

    @staticmethod
    def count_by_group(
        df: DataFrame, group_cols: List[str], count_col: str = None
    ) -> DataFrame:
        """
        Count records by group.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            count_col: Column to count, if None counts all records

        Returns:
            DataFrame with counts
        """
        if count_col:
            return df.groupBy(*group_cols).agg(count(col(count_col)).alias("count"))
        else:
            return df.groupBy(*group_cols).count()

    @staticmethod
    def distinct_count_by_group(
        df: DataFrame, group_cols: List[str], distinct_cols: List[str]
    ) -> DataFrame:
        """
        Count distinct values by group.

        Args:
            df: Input DataFrame
            group_cols: Columns to group by
            distinct_cols: Columns to count distinct values for

        Returns:
            DataFrame with distinct counts
        """
        agg_exprs = [
            countDistinct(col(col_name)).alias(f"distinct_{col_name}")
            for col_name in distinct_cols
        ]

        return df.groupBy(*group_cols).agg(*agg_exprs)

    @staticmethod
    def time_based_aggregation(
        df: DataFrame,
        date_col: str,
        time_unit: str = "day",
        value_cols: List[str] = None,
    ) -> DataFrame:
        """
        Aggregate data by time periods.

        Args:
            df: Input DataFrame
            date_col: Date column name
            time_unit: Time unit ('day', 'week', 'month', 'quarter', 'year')
            value_cols: Columns to aggregate

        Returns:
            DataFrame aggregated by time period
        """
        # Add time period column
        if time_unit == "day":
            df = df.withColumn("time_period", col(date_col))
        elif time_unit == "week":
            df = df.withColumn("time_period", date_format(col(date_col), "yyyy-ww"))
        elif time_unit == "month":
            df = df.withColumn("time_period", date_format(col(date_col), "yyyy-MM"))
        elif time_unit == "quarter":
            df = (
                df.withColumn("quarter", quarter(col(date_col)))
                .withColumn("year", year(col(date_col)))
                .withColumn(
                    "time_period", concat(col("year"), lit("-Q"), col("quarter"))
                )
            )
        elif time_unit == "year":
            df = df.withColumn("time_period", year(col(date_col)))
        else:
            raise ValueError(f"Unsupported time unit: {time_unit}")

        # Aggregate by time period
        if value_cols:
            agg_exprs = []
            for col_name in value_cols:
                agg_exprs.extend(
                    [
                        count(col(col_name)).alias(f"{col_name}_count"),
                        spark_sum(col(col_name)).alias(f"{col_name}_sum"),
                        avg(col(col_name)).alias(f"{col_name}_avg"),
                    ]
                )
            return df.groupBy("time_period").agg(*agg_exprs)
        else:
            return df.groupBy("time_period").count()

    @staticmethod
    def add_ranking(
        df: DataFrame,
        partition_cols: List[str],
        order_cols: List[str],
        ascending: bool = False,
    ) -> DataFrame:
        """
        Add ranking column to DataFrame.

        Args:
            df: Input DataFrame
            partition_cols: Columns to partition by
            order_cols: Columns to order by
            ascending: Whether to sort in ascending order

        Returns:
            DataFrame with ranking column
        """
        from pyspark.sql.functions import row_number

        window_spec = Window.partitionBy(*partition_cols)

        if ascending:
            window_spec = window_spec.orderBy(*[asc(col) for col in order_cols])
        else:
            window_spec = window_spec.orderBy(*[desc(col) for col in order_cols])

        return df.withColumn("rank", row_number().over(window_spec))

    @staticmethod
    def calculate_percentiles(
        df: DataFrame, numeric_col: str, percentiles: List[float] = [0.25, 0.5, 0.75]
    ) -> Dict[str, float]:
        """
        Calculate percentiles for a numeric column.

        Args:
            df: Input DataFrame
            numeric_col: Numeric column name
            percentiles: List of percentiles to calculate (0.0 to 1.0)

        Returns:
            Dictionary of percentile values
        """
        percentile_values = df.select(
            *[col(numeric_col).quantile(p).alias(f"p{int(p*100)}") for p in percentiles]
        ).collect()[0]

        return {
            f"p{int(p*100)}": percentile_values[f"p{int(p*100)}"] for p in percentiles
        }

    @staticmethod
    def add_moving_average(
        df: DataFrame,
        value_col: str,
        partition_cols: List[str],
        order_cols: List[str],
        window_size: int = 3,
    ) -> DataFrame:
        """
        Add moving average column.

        Args:
            df: Input DataFrame
            value_col: Column to calculate moving average for
            partition_cols: Columns to partition by
            order_cols: Columns to order by
            window_size: Size of the moving window

        Returns:
            DataFrame with moving average column
        """
        window_spec = (
            Window.partitionBy(*partition_cols)
            .orderBy(*order_cols)
            .rowsBetween(-window_size + 1, 0)
        )

        return df.withColumn(
            f"{value_col}_moving_avg_{window_size}",
            spark_round(avg(col(value_col)).over(window_spec), 2),
        )

    @staticmethod
    def pivot_summary(
        df: DataFrame,
        group_col: str,
        pivot_col: str,
        value_col: str,
        agg_func: str = "sum",
    ) -> DataFrame:
        """
        Create pivot table summary.

        Args:
            df: Input DataFrame
            group_col: Column to group by
            pivot_col: Column to pivot on
            value_col: Column with values to aggregate
            agg_func: Aggregation function ('sum', 'avg', 'count', 'max', 'min')

        Returns:
            Pivoted DataFrame
        """
        agg_functions = {
            "sum": spark_sum,
            "avg": avg,
            "count": count,
            "max": spark_max,
            "min": spark_min,
        }

        if agg_func not in agg_functions:
            raise ValueError(f"Unsupported aggregation function: {agg_func}")

        return (
            df.groupBy(group_col)
            .pivot(pivot_col)
            .agg(agg_functions[agg_func](col(value_col)))
        )

    @staticmethod
    def calculate_growth_rate(
        df: DataFrame, value_col: str, partition_cols: List[str], order_cols: List[str]
    ) -> DataFrame:
        """
        Calculate period-over-period growth rate.

        Args:
            df: Input DataFrame
            value_col: Column to calculate growth for
            partition_cols: Columns to partition by
            order_cols: Columns to order by

        Returns:
            DataFrame with growth rate column
        """
        from pyspark.sql.functions import lag

        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)

        df = df.withColumn("prev_value", lag(col(value_col)).over(window_spec))

        return df.withColumn(
            f"{value_col}_growth_rate",
            spark_round(
                ((col(value_col) - col("prev_value")) / col("prev_value")) * 100, 2
            ),
        ).drop("prev_value")
