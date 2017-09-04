package engine.core.sparkexpr.expr.aggregator

import engine.core.sparkexpr.expr.ExprHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by xiangnanren on 04/09/2017.
  */


/**
  * The Max aggregator bases on the type Double for computation,
  * i.e., the method converts BigDecimal, Double, Float, Int
  * into Double for aggregation.
  *
  * Note that the chosen initial value for Max aggregator is
  * Double.MinValue, i.e., negative infinity (-1.7976931348623157E308)
  */
object SparkExprMax extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("inputColumn", StringType) :: Nil)

  override def bufferSchema: StructType = {
    StructType(StructField("spark-agg-max", DoubleType) :: Nil)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Double.MinValue
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    if (!input.isNullAt(0)) {
      ExprHelper.isNumValue(input.getString(0)) match {
        case true =>
          val num = ExprHelper.getNumValueAsString(input.getString(0))(1).toDouble
          buffer(0) = if (buffer.getDouble(0) > num) buffer.getDouble(0)
          else num
        case false =>
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = if (buffer1.getDouble(0) > buffer2.getDouble(0))
      buffer1.getDouble(0)
    else buffer2.getDouble(0)
  }

  override def evaluate(buffer: Row): Double = buffer.getDouble(0)
}
