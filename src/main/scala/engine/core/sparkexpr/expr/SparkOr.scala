package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_LogicalOr

/**
  * Created by xiangnanren on 31/05/2017.
  */
class SparkOr(@transient val or: E_LogicalOr,
              leftExpr: SparkExpr,
              rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {
  override def execute(exprName: String,
                       leftChild: Any,
                       rightChild: Any): Boolean =
    (leftChild, rightChild) match {
      case (l: Boolean, r: Boolean) => l || r
    }


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkOr {
  def apply(@transient or: E_LogicalOr,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkOr = new SparkOr(or, leftExpr, rightExpr)
}