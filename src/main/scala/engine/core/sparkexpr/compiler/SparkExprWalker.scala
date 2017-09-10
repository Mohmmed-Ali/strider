package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._
import engine.core.sparkexpr.expr.aggregator.SparkExprAggregator

/**
  * Created by xiangnanren on 24/05/2017.
  */
private[compiler] class SparkExprWalker(val visitor: SparkExprVisitor)
  extends SparkExprVisitorByType {

  def walkBottomUp(expr: SparkExpr): Unit = {
    expr.visit(new SparkExprWalker(visitor))
  }

  override def visit1(expr: SparkExpr1[SparkExpr]): Unit = {
    if (Option(expr.subExpr).nonEmpty) expr.subExpr.visit(this)
    expr.visit(visitor)
  }

  override def visit2(expr: SparkExpr2[SparkExpr, SparkExpr]): Unit = {
    if (Option(expr.leftExpr).nonEmpty) expr.leftExpr.visit(this)
    if (Option(expr.rightExpr).nonEmpty) expr.rightExpr.visit(this)

    expr.visit(visitor)
  }

  override def visit(expr: SparkExprAggregator): Unit = {
    expr.visit(visitor)
  }

  override def visit(expr: SparkExprVar): Unit = {
    expr.visit(visitor)
  }

  override def visit(expr: SparkNodeValue): Unit = {
    expr.visit(visitor)
  }
}


private[compiler] object SparkExprWalker {
  def apply(visitor: SparkExprVisitor): SparkExprWalker =
    new SparkExprWalker(visitor)
}