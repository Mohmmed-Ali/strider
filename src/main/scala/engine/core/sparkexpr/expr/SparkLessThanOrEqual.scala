package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_LessThanOrEqual

/**
  * Created by xiangnanren on 31/05/2017.
  */
class SparkLessThanOrEqual(@transient val expr: E_LessThanOrEqual,
                           leftExpr: SparkExpr,
                           rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  override def execute(exprName: String,
                       leftChild: Any,
                       rightExpr: Any): Boolean =
    (leftChild, rightExpr) match {
      case (l: Number, r: Number) => BigDecimal(l.toString).<=(BigDecimal(r.toString))
    }


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkLessThanOrEqual {
  def apply(@transient expr: E_LessThanOrEqual,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkLessThanOrEqual =
    new SparkLessThanOrEqual(expr, leftExpr, rightExpr)
}