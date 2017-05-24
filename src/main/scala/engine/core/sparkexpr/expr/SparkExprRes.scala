package engine.core.sparkexpr.expr

/**
  * Created by xiangnanren on 24/05/2017.
  */
abstract class SparkExprRes {

}

case class IntExprRes(res: Int) extends SparkExprRes

case class LongExprRes(res: Long) extends SparkExprRes

case class FloatExprRes(res: Float) extends SparkExprRes

case class DoubleExprRes(res: Double) extends SparkExprRes

case class StringExprRes(res: String) extends SparkExprRes

case class BoolExprRes(res: Boolean) extends SparkExprRes