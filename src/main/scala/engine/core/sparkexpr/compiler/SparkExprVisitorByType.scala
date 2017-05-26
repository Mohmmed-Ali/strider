package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._

/**
  * Created by xiangnanren on 24/05/2017.
  */
abstract class SparkExprVisitorByType extends SparkExprVisitor{

  override def visit(sparkEquals: SparkEquals): Unit = {
    visit2(sparkEquals)
  }

  override def visit(sparkExprVar: SparkExprVar): Unit = {
    visit(sparkExprVar)
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {
    visit(sparkNodeValue)
  }


  def visit1(sparkExpr1: SparkExpr1[SparkExpr])

  def visit2(sparkExpr2: SparkExpr2[SparkExpr, SparkExpr])


}
