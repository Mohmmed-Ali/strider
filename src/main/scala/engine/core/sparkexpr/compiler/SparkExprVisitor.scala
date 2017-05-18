package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._

/**
  * Created by xiangnanren on 03/05/2017.
  */
trait SparkExprVisitor {

  def visit(sparkAdd: SparkAdd): Unit = {}

  def visit(sparkAnd: SparkAnd): Unit = {}

  def visit(sparkBound: SparkBound): Unit = {}

  def visit(sparkExprVar: SparkExprVar): Unit = {}

  def visit(sparkGreaterThan: SparkGreaterThan): Unit = {}

  def visit(sparkLessThan: SparkLessThan): Unit = {}

  def visit(sparkNodeValue: SparkNodeValue): Unit = {}

  def visit(sparkNot: SparkNot): Unit = {}

}
