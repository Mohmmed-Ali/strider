package engine.core.sparkexpr.expr

/**
  * Created by xiangnanren on 03/05/2017.
  */
abstract class SparkExpr1[T <: SparkExpr](val subExpr: T) extends SparkExpr{

}
