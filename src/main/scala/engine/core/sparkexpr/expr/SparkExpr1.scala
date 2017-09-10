package engine.core.sparkexpr.expr

/**
  * Created by xiangnanren on 03/05/2017.
  */
private[sparkexpr] abstract class SparkExpr1
[T <: SparkExpr](val subExpr: T) extends SparkExpr {
  def execute(exprName: String,
              subExpr: Any): Any

}
