package engine.core.optimizer

import engine.core.sparkop.compiler.{SparkOpVisitorBase, SparkOpWalker}
import engine.core.sparkop.op._

/**
  * Created by xiangnanren on 10/10/16.
  */
class OptimAlgebraResolver extends SparkOpVisitorBase {
  private val stack = new scala.collection.mutable.Stack[SparkOp]

  def optimize(op: SparkOp): SparkOp = {
    SparkOpWalker(this).walkBottomUp(op)
    stack.pop()
  }

  override def visit(sparkBGP: SparkBGP): Unit = {

    stack.push(sparkBGP)
  }

  override def visit(sparkFilter: SparkFilter): Unit = {}

  override def visit(sparkGroup: SparkGroup): Unit = {}

  override def visit(sparkJoin: SparkJoin): Unit = {}

  override def visit(sparkLeftJoin: SparkLeftJoin): Unit = {}

  override def visit(sparkSequence: SparkSequence): Unit = {}

  override def visit(sparkUnion: SparkUnion): Unit = {}

  override def visit(sparkDistinct: SparkDistinct): Unit = {}

  override def visit(sparkProject: SparkProjection): Unit = {}

}

object OptimAlgebraResolver {
  def apply(): OptimAlgebraResolver = new OptimAlgebraResolver()
}
