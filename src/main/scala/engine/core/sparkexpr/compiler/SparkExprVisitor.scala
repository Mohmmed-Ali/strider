package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._
import engine.core.sparkexpr.expr.aggregator.SparkExprAggregator

/**
  * Created by xiangnanren on 03/05/2017.
  */
trait SparkExprVisitor {

  def visit(sparkAdd: SparkAdd): Unit = {}

  def visit(sparkExprAggregator: SparkExprAggregator): Unit = {}

  def visit(sparkAnd: SparkAnd): Unit = {}

  def visit(sparkBound: SparkBound): Unit = {}

  def visit(sparkEquals: SparkEquals): Unit = {}

  def visit(sparkExprVar: SparkExprVar): Unit = {}

  def visit(sparkGreaterThan: SparkGreaterThan): Unit = {}

  def visit(sparkGreaterThanOrEqual: SparkGreaterThanOrEqual): Unit = {}

  def visit(sparkLessThan: SparkLessThan): Unit = {}

  def visit(sparkLessThanOrEqual: SparkLessThanOrEqual): Unit = {}

  def visit(sparkNodeValue: SparkNodeValue): Unit = {}

  def visit(sparkNot: SparkNot): Unit = {}

  def visit(sparkNotEquals: SparkNotEquals): Unit = {}

  def visit(sparkOr: SparkOr): Unit = {}

}
