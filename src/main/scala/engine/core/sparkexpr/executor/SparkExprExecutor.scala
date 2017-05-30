package engine.core.sparkexpr.executor

import engine.core.sparkexpr.compiler.{SparkExprVisitor, SparkExprWalker}
import engine.core.sparkexpr.expr._

/**
  * Created by xiangnanren on 19/05/2017.
  */

/**
  *
  * @param arg: Argument of a given DataFrame.
  *             "arg" is used as the input for the user defined function.
  */
class SparkExprExecutor(arg: String) extends SparkExprVisitor {
  private[this] val stack = new scala.collection.mutable.Stack[Any]

  /**
    * This method is nested in the user defined function and
    * computes the result of a given expression.
    *
    * @param expr: Root of the compiled expression tree
    */
  def execute(expr: SparkExpr): Any = {
    SparkExprWalker(this).walkBottomUp(expr)
    stack.pop()
  }

  override def visit(sparkExprVar: SparkExprVar): Unit = {
    stack.push(sparkExprVar.execute(arg))
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {
    stack.push(sparkNodeValue.valueMapping)
  }

  override def visit(sparkEquals: SparkEquals): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()
    stack.push(
      sparkEquals.
        execute(sparkEquals.exprName, leftChild, rightChild))
  }
}


object SparkExprExecutor {
  def apply(arg: String): SparkExprExecutor = new SparkExprExecutor(arg)
}