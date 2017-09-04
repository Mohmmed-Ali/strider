package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._
import engine.core.sparkexpr.expr.aggregator.SparkExprAggregator
import engine.core.sparkop.compiler.SparkOpTransformer
import org.apache.jena.sparql.expr._
import org.apache.log4j.LogManager

import scala.collection.mutable

/**
  * Created by xiangnanren on 03/05/2017.
  */
class SparkExprTransformer extends ExprVisitor {
  @transient
  private[this] val log = LogManager.
    getLogger(SparkOpTransformer.getClass)

  private val stack = new mutable.Stack[SparkExpr]
  private var exprID = -1

  def transform(expr: Expr): SparkExpr = {
    OriginalExprWalker(this).walkBottomUp(expr)
    stack.pop()
  }

  def visit(func: ExprFunction): Unit = func match {
    case f: ExprFunction0 => visit(f)
    case f: ExprFunction1 => visit(f)
    case f: ExprFunction2 => visit(f)
    case f: ExprFunction3 => visit(f)
    case f: ExprFunctionN => visit(f)
    case f: ExprFunctionOp => visit(f)
    }

  override def visit(func: ExprFunction0): Unit = {
    throw new UnsupportedOperationException("ExprFunction0 not supported yet.")
  }

  override def visit(func: ExprFunction1): Unit = {
    val subExpr = stack.pop()

    val tempExpr = func match {
      case f: E_Bound => exprID += 1
        SparkBound(f, subExpr)

      case f: E_LogicalNot => exprID += 1
        SparkNot(f, subExpr)
    }

    stack.push(tempExpr)
  }

  override def visit(func: ExprFunction2): Unit = {
    val rightExpr = stack.pop()
    val leftExpr = stack.pop()
    exprID += 1

    val tempExpr = func match {
      case f: E_Add => log.debug(s"exprID: $exprID, E_Add: $f")
        SparkAdd(f, leftExpr, rightExpr)

      case f: E_Equals => log.debug(s"exprID: $exprID, E_Equals: $f")
        SparkEquals(f, leftExpr, rightExpr)

      case f: E_GreaterThan => log.debug(s"exprID: $exprID, E_GreaterThan: $f")
        SparkGreaterThan(f, leftExpr, rightExpr)

      case f: E_GreaterThanOrEqual => log.debug(s"exprID: $exprID, E_GreaterThanOrEqual: $f")
        SparkGreaterThanOrEqual(f, leftExpr, rightExpr)

      case f: E_LogicalAnd => log.debug(s"exprID: $exprID, E_LogicalAnd: $f")
        SparkAnd(f, leftExpr, rightExpr)

      case f: E_LogicalOr => log.debug(s"exprID: $exprID, E_LogicalOr: $f")
        SparkOr(f, leftExpr, rightExpr)

      case f: E_LessThan => log.debug(s"exprID: $exprID, E_LessThan: $f")
        SparkLessThan(f, leftExpr, rightExpr)

      case f: E_LessThanOrEqual => log.debug(s"exprID: $exprID, E_LessThanOrEqual: $f")
        SparkLessThanOrEqual(f, leftExpr, rightExpr)

      case f: E_NotEquals => log.debug(s"exprID: $exprID, E_NotEquals: $f")
        SparkNotEquals(f, leftExpr, rightExpr)

    }
    stack.push(tempExpr)
  }

  override def visit(func: ExprFunction3): Unit = {}

  override def visit(func: ExprFunctionN): Unit = {}

  override def visit(funcOp: ExprFunctionOp): Unit = {}

  override def visit(nv: NodeValue): Unit = {
    exprID += 1
    log.debug(s"exprID: $exprID, nv: ${nv.asQuotedString}")

    stack.push(SparkNodeValue(nv))
  }

  override def visit(exprVar: ExprVar): Unit = {
    exprID += 1
    log.debug(s"exprID: $exprID, exprVar: ${exprVar.getVarName}")

    stack.push(SparkExprVar(exprVar))
  }

  override def visit(eAgg: ExprAggregator): Unit = {
    exprID += 1
    log.debug(s"exprID: $exprID, eAgg: $eAgg")

    println("visit: " + eAgg)

    stack.push(SparkExprAggregator(eAgg))
  }

  override def finishVisit(): Unit = {}

  override def startVisit(): Unit = {}
}
