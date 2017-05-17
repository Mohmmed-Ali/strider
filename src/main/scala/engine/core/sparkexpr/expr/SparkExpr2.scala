package engine.core.sparkexpr.expr

/**
  * Created by xiangnanren on 03/05/2017.
  */
abstract class SparkExpr2[S, T <: SparkExpr](val leftExpr: S,
                                             val rightExpr: T) extends SparkExpr {

}
