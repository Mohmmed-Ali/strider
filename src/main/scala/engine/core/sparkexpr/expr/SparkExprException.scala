package engine.core.sparkexpr.expr

/**
  * Created by xiangnanren on 25/05/2017.
  */
sealed abstract class SparkExprException extends Exception

case class NullExprException(msg: String) extends Exception(msg)
case class VarOutOfBoundException(msg: String) extends Exception(msg)
case class UnsupportedLiteralException(msg: String) extends Exception(msg)