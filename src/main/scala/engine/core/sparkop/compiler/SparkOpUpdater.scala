package engine.core.sparkop.compiler

import engine.core.sparkop.op.{SparkBGP, SparkOp}
import org.apache.spark.sql._

/**
  * Created by xiangnanren on 22/03/2017.
  */
class SparkOpUpdater(inputDF: DataFrame) extends SparkOpVisitor {
  def update(op: SparkOp): Unit = {
    SparkOpWalker(this).walkBottomUp(op)
  }

  override def visit(sparkBGP: SparkBGP): Unit = {
    sparkBGP.update(sparkBGP.opName, inputDF)
  }

}

object SparkOpUpdater {
  def apply(inputDF: DataFrame): SparkOpUpdater = new SparkOpUpdater(inputDF)
}