package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.ExprVar

/**
  * Created by xiangnanren on 05/05/2017.
  */
class SparkExprVar(val expr: ExprVar) extends SparkExpr {
  val exprVar = expr.getVarName

  def execute(arg: Any): Any = arg

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkExprVar {
  def apply(expr: ExprVar): SparkExprVar = new SparkExprVar(expr)
}