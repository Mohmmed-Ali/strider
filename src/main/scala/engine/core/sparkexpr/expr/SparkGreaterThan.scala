package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_GreaterThan

/**
  * Created by xiangnanren on 03/05/2017.
  */
private[sparkexpr] class SparkGreaterThan(@transient val expr: E_GreaterThan,
                                          leftExpr: SparkExpr,
                                          rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  override def execute(exprName: String, leftChild: Any, rightExpr: Any): Boolean = {

    (leftChild, rightExpr) match {
      case (l: Number, r: Number) => l.doubleValue() > r.doubleValue()
      case (l: Number, r) => l.doubleValue() > r.toString.toDouble
      case (l, r: Number) => l.toString.toDouble > r.doubleValue()

    }
  }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}


object SparkGreaterThan {
  def apply(@transient expr: E_GreaterThan,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkGreaterThan = new SparkGreaterThan(expr, leftExpr, rightExpr)

}