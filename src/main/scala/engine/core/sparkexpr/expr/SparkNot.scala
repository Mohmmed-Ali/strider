package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_LogicalNot

/**
  * Created by xiangnanren on 03/05/2017.
  */
class SparkNot(val expr: E_LogicalNot,
               subExpr: SparkExpr) extends SparkExpr1[SparkExpr](subExpr) {


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }

}

object SparkNot {
  def apply(expr: E_LogicalNot,
            subExpr: SparkExpr): SparkNot = new SparkNot(expr, subExpr)
}