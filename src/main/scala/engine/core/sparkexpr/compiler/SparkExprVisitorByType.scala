package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._

/**
  * Created by xiangnanren on 24/05/2017.
  */
abstract class SparkExprVisitorByType extends SparkExprVisitor{

  def visit(sparkExpr1: SparkExpr1[SparkExpr]): Unit = {
    visit(sparkExpr1)
  }

  def visit(sparkExpr2: SparkExpr2[SparkExpr, SparkExpr]): Unit = {
    visit(sparkExpr2)
  }

  override def visit(sparkExprVar: SparkExprVar): Unit = {
    visit(sparkExprVar)
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {
    visit(sparkNodeValue)
  }
}
