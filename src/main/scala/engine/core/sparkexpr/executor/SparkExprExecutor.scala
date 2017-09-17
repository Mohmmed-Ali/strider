package engine.core.sparkexpr.executor

import engine.core.sparkexpr.compiler.{SparkExprVisitor, SparkExprWalker}
import engine.core.sparkexpr.expr.{SparkExprAggregator, _}

/**
  * Created by xiangnanren on 19/05/2017.
  */

/**
  *
  * @param args : Argument of a given DataFrame.
  *            "args" is used as the input for the user defined function.
  */
class SparkExprExecutor(orderedColumnNames: Vector[String],
                        args: String*) extends SparkExprVisitor {
  private[this] val stack = new scala.collection.mutable.Stack[Any]

  def this(args: String*) = this(Vector.empty[String], args:_*)

  /**
    * This method is nested in the user defined function and
    * computes the result of a given expression.
    *
    * @param expr : Root of the compiled expression tree
    */
  def execute(expr: SparkExpr): Any = {
    SparkExprWalker(this).walkBottomUp(expr)
    stack.pop()
  }

  override def visit(sparkAdd: SparkAdd): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkAdd.
        execute(sparkAdd.exprName, leftChild, rightChild))
  }

  override def visit(sparkExprAggregator: SparkExprAggregator): Unit = {
    ???
  }

  override def visit(sparkAnd: SparkAnd): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkAnd.
        execute(sparkAnd.exprName, leftChild, rightChild)
    )
  }

  override def visit(sparkBound: SparkBound): Unit = {
    stack.push(
      sparkBound.
        execute(sparkBound.exprName, stack.pop()))
  }

  override def visit(sparkEquals: SparkEquals): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkEquals.
        execute(sparkEquals.exprName, leftChild, rightChild))
  }

  override def visit(sparkExprVar: SparkExprVar): Unit = {

    val index = orderedColumnNames.indexOf(sparkExprVar.varName)
    //    println("visit(sparkExprVar: SparkExprVar): " + index)
    stack.push(sparkExprVar.execute(args(index)))
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {
    stack.push(sparkNodeValue.valueMapping)
  }

  override def visit(sparkGreaterThan: SparkGreaterThan): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkGreaterThan.
        execute(sparkGreaterThan.exprName, leftChild, rightChild))
  }

  override def visit(sparkGreaterThanOrEqual: SparkGreaterThanOrEqual): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkGreaterThanOrEqual.
        execute(sparkGreaterThanOrEqual.exprName, leftChild, rightChild))
  }

  override def visit(sparkLessThan: SparkLessThan): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkLessThan.
        execute(sparkLessThan.exprName, leftChild, rightChild)
    )
  }

  override def visit(sparkLessThanOrEqual: SparkLessThanOrEqual): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkLessThanOrEqual.
        execute(sparkLessThanOrEqual.exprName, leftChild, rightChild)
    )
  }

  override def visit(sparkNot: SparkNot): Unit = {
    stack.push(
      sparkNot.
        execute(sparkNot.exprName, stack.pop())
    )
  }

  override def visit(sparkNotEquals: SparkNotEquals): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkNotEquals.
        execute(sparkNotEquals.exprName, leftChild, rightChild)
    )
  }

  override def visit(sparkOr: SparkOr): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkOr.
        execute(sparkOr.exprName, leftChild, rightChild)
    )
  }

  override def visit(sparkSubtract: SparkSubtract): Unit = {
    val rightChild = stack.pop()
    val leftChild = stack.pop()

    stack.push(
      sparkSubtract.
        execute(sparkSubtract.exprName, leftChild, rightChild))
  }

}

object SparkExprExecutor {
  def apply(orderedColumnNames: Vector[String],
            args: String*): SparkExprExecutor =
    new SparkExprExecutor(orderedColumnNames, args:_*)

}