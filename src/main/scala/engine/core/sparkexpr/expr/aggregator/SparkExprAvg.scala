package engine.core.sparkexpr.expr.aggregator

import engine.core.sparkexpr.expr.ExprHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DoubleType, LongType, _}

/**
  * Created by xiangnanren on 04/09/2017.
  */
private[sparkexpr] object SparkExprAvg
  extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("inputColumn", StringType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(
      StructField("spark-agg-sum", DoubleType) ::
        StructField("spark-agg-count", LongType) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      ExprHelper.isNumValue(input.getString(0)) match {
        case true =>
          val num = ExprHelper.getNumValueAsString(input.getString(0))(1).toDouble
          buffer(0) = buffer.getDouble(0) + num
          buffer(1) = buffer.getLong(1) + 1

        case false =>
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Double = buffer.getDouble(0) / buffer.getLong(1)
}
