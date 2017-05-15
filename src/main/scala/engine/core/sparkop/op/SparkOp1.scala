package engine.core.sparkop.op

/**
  * Created by xiangnanren on 07/07/16.
  */
abstract class SparkOp1[T <: SparkOp](val subOp: T) extends
  SparkOpBase {

  def execute(opName: String,
              child: SparkOpRes): SparkOpRes

}
