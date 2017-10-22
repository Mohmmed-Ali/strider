package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_Subtract

/**
  * Created by xiangnanren on 24/06/2017.
  */
private[sparkexpr] class SparkSubtract (@transient val subtract: E_Subtract,
                                        leftExpr: SparkExpr,
                                        rightExpr: SparkExpr)
  extends SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  /**
    * The computation for subtraction bases on double type
    */
  override def execute(exprName: String,
                       leftChild: Any,
                       rightChild: Any): Double =
    (leftChild, rightChild) match {
      case (l: Number, r: Number) => l.doubleValue() - r.doubleValue()
      case (l: String, r: String) => l.toDouble - r.toDouble
    }


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

private[sparkexpr] object SparkSubtract {
  def apply(@transient subtract: E_Subtract,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkSubtract =
    new SparkSubtract(subtract, leftExpr, rightExpr)

}