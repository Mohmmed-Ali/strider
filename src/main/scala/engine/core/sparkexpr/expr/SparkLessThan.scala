package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_LessThan

/**
  * Created by xiangnanren on 03/05/2017.
  */
class SparkLessThan(@transient val expr: E_LessThan,
                    leftExpr: SparkExpr,
                    rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  override def execute(exprName: String,
                       leftChild: Any,
                       rightExpr: Any): Boolean =
    (leftChild, rightExpr) match {
      case (l: Number, r: Number) => BigDecimal(l.toString).<(BigDecimal(r.toString))
    }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkLessThan {
  def apply(@transient expr: E_LessThan,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkLessThan =
    new SparkLessThan(expr, leftExpr, rightExpr)

}
