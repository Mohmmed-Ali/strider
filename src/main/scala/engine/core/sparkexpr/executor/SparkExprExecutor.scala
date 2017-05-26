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
class SparkExprExecutor(arg: Any) extends SparkExprVisitor {
  private[this] val stack = new scala.collection.mutable.Stack[Any]

  /**
    * This method is nested in the user defined function and
    * computes the result of a given expression.
    *
    * @param expr: Root of the compiled expression tree
    */
  def execute(expr: SparkExpr): Any = {
    SparkExprWalker(this).walkBottomUp(expr)

    println("stack pop " +  stack.isEmpty)

    stack.pop()
  }

  override def visit(sparkExprVar: SparkExprVar): Unit = {

    println("SparkExprVar in " + arg)

    stack.push(sparkExprVar.execute(arg))
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {

    println("SparkNodeValue in stack: " + sparkNodeValue.valueMapping)

    stack.push(sparkNodeValue)
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
  def apply(arg: Any): SparkExprExecutor = new SparkExprExecutor(arg)
}