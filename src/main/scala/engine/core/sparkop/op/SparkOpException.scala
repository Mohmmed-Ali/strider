package engine.core.sparkop.op

/**
  * Created by xiangnanren on 17/05/2017.
  */
sealed abstract class SparkOpException extends Exception

case class NullUCGException(msg: String) extends Exception(msg)
