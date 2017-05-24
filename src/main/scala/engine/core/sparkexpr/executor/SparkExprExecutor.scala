package engine.core.sparkexpr.executor

import engine.core.sparkexpr.compiler.{SparkExprVisitorByType, SparkExprWalker}
import engine.core.sparkexpr.expr.{ExprResMapping, SparkExpr}

/**
  * Created by xiangnanren on 19/05/2017.
  */
class SparkExprExecutor() extends SparkExprVisitorByType {
  private[this] val stack = new scala.collection.mutable.Stack[ExprResMapping]

  def execute(expr: SparkExpr): ExprResMapping = {
    SparkExprWalker(this).walkBottomUp(expr)
    stack.pop()
  }

  
}
