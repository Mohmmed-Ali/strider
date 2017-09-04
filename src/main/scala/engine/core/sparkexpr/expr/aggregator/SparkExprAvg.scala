package engine.core.sparkexpr.expr.aggregator

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * Created by xiangnanren on 04/09/2017.
  */
object SparkExprAvg extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def bufferSchema: StructType = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def deterministic: Boolean = ???

  override def evaluate(buffer: Row): Any = ???

  override def dataType: DataType = ???
}
