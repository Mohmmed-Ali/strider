package engine.core.sparkop.compiler

import engine.core.sparkop.op._

/**
  * Created by xiangnanren on 07/07/16.
  */
class SparkOpWalker(val visitor: SparkOpVisitor)
  extends SparkOpVisitorByType {

  def walkBottomUp(op: SparkOp): Unit = {
    op.visit(new SparkOpWalker(visitor))
  }

  def visit0(op: SparkOp0): Unit = {
    op.visit(visitor)
  }

  def visit1(op: SparkOp1[SparkOp]): Unit = {
    if (Option(op.subOp).nonEmpty) op.subOp.visit(this)
    op.visit(visitor)
  }

  def visit2(op: SparkOp2[SparkOp, SparkOp]): Unit = {
    if (Option(op.leftOp).nonEmpty) op.leftOp.visit(this)
    if (Option(op.rightOp).nonEmpty) op.rightOp.visit(this)
    op.visit(visitor)
  }

  def visitN(op: SparkOpN): Unit = {

  }
}

object SparkOpWalker {
  def apply(visitor: SparkOpVisitor): SparkOpWalker = new SparkOpWalker(visitor)
}