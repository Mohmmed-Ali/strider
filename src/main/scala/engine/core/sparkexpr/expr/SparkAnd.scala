package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_LogicalAnd

/**
  * Created by xiangnanren on 18/05/2017.
  */
private[sparkexpr] class SparkAnd(@transient val expr: E_LogicalAnd,
                                  leftExpr: SparkExpr,
                                  rightExpr: SparkExpr)
  extends SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  override def execute(exprName: String,
                       leftChild: Any,
                       rightExpr: Any): Boolean =
    (leftChild, rightExpr) match {
      case (l: Boolean, r: Boolean) => l && r
    }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}


private[sparkexpr] object SparkAnd {
  def apply(@transient expr: E_LogicalAnd,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkAnd = new SparkAnd(expr, leftExpr, rightExpr)
}