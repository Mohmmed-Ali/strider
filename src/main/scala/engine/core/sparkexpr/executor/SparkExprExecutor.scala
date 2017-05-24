package engine.core.sparkexpr.executor

import engine.core.sparkexpr.compiler.{SparkExprVisitorByType, SparkExprWalker}
import engine.core.sparkexpr.expr.{ExprResMapping, SparkExpr, SparkNodeValue}

/**
  * Created by xiangnanren on 19/05/2017.
  */
class SparkExprExecutor(arg: Any) extends SparkExprVisitorByType {
  private[this] val stack = new scala.collection.mutable.Stack[ExprResMapping]

  def execute(expr: SparkExpr): ExprResMapping = {
    SparkExprWalker(this).walkBottomUp(expr)
    stack.pop()
  }

  override def visit(sparkNodeValue: SparkNodeValue): Unit = {
    stack.push(sparkNodeValue.resMapping)
  }


}
