package engine.core.sparkop.op

import engine.core.sparkexpr.executor._
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

/**
  * Created by xiangnanren on 13/09/2017.
  */
trait SparkAggUDF {
  val aggregator: UserDefinedAggregateFunction
}

case object AggMaxWrapper extends SparkAggUDF {
  val aggregator: SparkAggMax.type = SparkAggMax
}

case object AggMinWrapper extends SparkAggUDF {
  val aggregator: SparkAggMin.type = SparkAggMin
}

case object AggAvgWrapper extends SparkAggUDF {
  val aggregator: SparkAggAvg.type = SparkAggAvg
}

case object AggSumWrapper extends SparkAggUDF {
  val aggregator: SparkAggSum.type = SparkAggSum
}

case object AggCountWrapper extends SparkAggUDF {
  val aggregator: SparkAggCount.type = SparkAggCount
}
