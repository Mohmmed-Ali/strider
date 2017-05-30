package engine.core.sparkexpr.executor

import engine.core.sparkexpr.expr.SparkExpr
import org.apache.spark.sql.functions.udf

/**
  * Created by xiangnanren on 25/05/2017.
  */
object ExprUDF {

  /**
    * UDF with boolean return-type
    *
    */
  def UDFWithBoolean(expr: SparkExpr) = udf(
    (arg: String) => {
      val res = SparkExprExecutor(arg).execute(expr)
      res match {
        case _res: Boolean => _res
      }
    })
}
