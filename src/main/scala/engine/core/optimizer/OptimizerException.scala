package engine.core.optimizer

/**
  * Created by xiangnanren on 29/11/2016.
  */
sealed abstract class OptimizerException(msg: String) extends Exception

case class InvalidEPException(msg: String) extends OptimizerException(msg)

case class EmptyUCGException(msg: String) extends OptimizerException(msg)
