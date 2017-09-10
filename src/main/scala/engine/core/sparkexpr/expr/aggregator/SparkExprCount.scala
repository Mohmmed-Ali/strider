package engine.core.sparkexpr.expr.aggregator

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by xiangnanren on 04/09/2017.
  */

/**
  * The Count aggregator bases on the type Long for computation
  *
  */
private[sparkexpr] object SparkExprCount
  extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("inputColumn", StringType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("spark-agg-count", LongType) :: Nil)
  }

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0))
      buffer(0) = buffer.getLong(0) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Long = buffer.getLong(0)
}
