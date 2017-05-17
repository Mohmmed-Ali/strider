package engine.core.sparkexpr.compiler

import engine.core.sparkexpr.expr._
import engine.core.sparkop.compiler.OpTransformer
import org.apache.jena.sparql.expr._
import org.apache.log4j.LogManager

import scala.collection.mutable

/**
  * Created by xiangnanren on 03/05/2017.
  */
class SparkExprTransformer extends ExprVisitor {
  @transient
  private[this] val log = LogManager.
    getLogger(OpTransformer.getClass)

  private val stack = new mutable.Stack[SparkExpr]
  private var exprID = -1

  def transform(expr: Expr): SparkExpr = {

    SparqlExprWalker(this).walkBottomUp(expr)
    stack.pop()
  }

  def visit(func: ExprFunction): Unit = {
    func match {
      case f: ExprFunction0 => visit(f)

      case f: ExprFunction1 => visit(f)

      case f: ExprFunction2 => visit(f)

      case f: ExprFunction3 => visit(f)

      case f: ExprFunctionN => visit(f)

      case f: ExprFunctionOp => visit(f)
    }
  }


  override def visit(func: ExprFunction0): Unit = {
    throw new UnsupportedOperationException("ExprFunction0 not supported yet.")
  }

  override def visit(func: ExprFunction1): Unit = {
    val subExpr = stack.pop()

    val tempExpr = func match {
      case f: E_Bound => SparkBound(f, subExpr)

      case f: E_LogicalNot => SparkNot(f, subExpr)
    }
    stack.push(tempExpr)
  }

  override def visit(func: ExprFunction2): Unit = {
    val rightExpr = stack.pop()
    val leftExpr = stack.pop()

    val tempExpr = func match {
      case f: E_Add => SparkAdd(f, leftExpr, rightExpr)

      case f: E_GreaterThan => SparkGreaterThan(f, leftExpr, rightExpr)

      case f: E_LessThan => SparkLessThan(f, leftExpr, rightExpr)
    }
    stack.push(tempExpr)
  }

  override def visit(func: ExprFunction3): Unit = {}

  override def visit(func: ExprFunctionN): Unit = {}

  override def visit(funcOp: ExprFunctionOp): Unit = {}

  override def visit(nv: NodeValue): Unit = {

  }

  override def visit(exprVar: ExprVar): Unit = {
    stack.push(SparkExprVar(exprVar))
  }

  override def visit(eAgg: ExprAggregator): Unit = {}

  override def finishVisit(): Unit = {}

  override def startVisit(): Unit = {}
}
