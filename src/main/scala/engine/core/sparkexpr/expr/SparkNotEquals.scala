package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_NotEquals

/**
  * Created by xiangnanren on 31/05/2017.
  */
private[sparkexpr] class SparkNotEquals
(@transient val notEquals: E_NotEquals,
 leftExpr: SparkExpr,
 rightExpr: SparkExpr)
  extends SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  override def execute(exprName: String,
                       leftChild: Any,
                       rightChild: Any): Boolean = leftChild != rightChild

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkNotEquals {
  def apply(@transient notEquals: E_NotEquals,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkNotEquals =
    new SparkNotEquals(notEquals, leftExpr, rightExpr)
}