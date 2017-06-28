package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._

/**
  * Created by xiangnanren on 24/05/2017.
  */
abstract class SparkExprVisitorByType extends SparkExprVisitor {

  override def visit(sparkAnd: SparkAnd): Unit = {
    visit2(sparkAnd)
  }

  override def visit(sparkBound: SparkBound): Unit = {
    visit1(sparkBound)
  }

  override def visit(sparkEquals: SparkEquals): Unit = {
    visit2(sparkEquals)
  }

  override def visit(sparkExprVar: SparkExprVar): Unit = {
    visit(sparkExprVar)
  }

  override def visit(sparkGreaterThan: SparkGreaterThan): Unit = {
    visit2(sparkGreaterThan)
  }

  override def visit(sparkGreaterThanOrEqual: SparkGreaterThanOrEqual): Unit = {
    visit2(sparkGreaterThanOrEqual)
  }

  override def visit(sparkLessThan: SparkLessThan): Unit = {
    visit2(sparkLessThan)
  }

  override def visit(sparkLessThanOrEqual: SparkLessThanOrEqual): Unit = {
    visit2(sparkLessThanOrEqual)
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {
    visit(sparkNodeValue)
  }

  override def visit(sparkNot: SparkNot): Unit = {
    visit1(sparkNot)
  }

  override def visit(sparkNotEquals: SparkNotEquals): Unit = {
    visit2(sparkNotEquals)
  }

  override def visit(sparkOr: SparkOr): Unit = {
    visit2(sparkOr)
  }


  def visit1(sparkExpr1: SparkExpr1[SparkExpr])

  def visit2(sparkExpr2: SparkExpr2[SparkExpr, SparkExpr])


}
