package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.ExprVar

/**
  * Created by xiangnanren on 05/05/2017.
  */
private[sparkexpr] class SparkExprVar(@transient val expr: ExprVar)
  extends SparkExpr {
  val varName = expr.getVarName
  val valueFieldPattern = "\".*?\"".r

  def execute(arg: String): Any = {
    if (ExprHelper.isQuotedString(arg))
      ExprHelper.getArgValue(arg)
    else arg
  }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

private[sparkexpr] object SparkExprVar {
  def apply(@transient expr: ExprVar): SparkExprVar = new SparkExprVar(expr)
}