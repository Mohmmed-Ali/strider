package engine.core.sparkop.op

/**
  * Created by xiangnanren on 08/07/16.
  */
abstract class SparkOp2[S, T <: SparkOp](val leftOp: S,
                                         val rightOp: T) extends SparkOpBase {

  def execute(opName: String,
              leftChild: SparkOpRes,
              rightChild: SparkOpRes): SparkOpRes
}
