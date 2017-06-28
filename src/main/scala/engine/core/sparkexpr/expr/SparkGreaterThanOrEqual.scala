package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_GreaterThanOrEqual

/**
  * Created by xiangnanren on 31/05/2017.
  */
class SparkGreaterThanOrEqual(@transient val expr: E_GreaterThanOrEqual,
                              leftExpr: SparkExpr,
                              rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {
  override def execute(exprName: String,
                       leftChild: Any,
                       rightExpr: Any): Boolean =
    (leftChild, rightExpr) match {
      case (l: Number, r: Number) => BigDecimal(l.toString).>=(BigDecimal(r.toString))
    }


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkGreaterThanOrEqual {
  def apply(@transient expr: E_GreaterThanOrEqual,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkGreaterThanOrEqual =
    new SparkGreaterThanOrEqual(expr, leftExpr, rightExpr)
}