package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_Add

/**
  * Created by xiangnanren on 03/05/2017.
  */
private[sparkexpr] class SparkAdd(@transient val add: E_Add,
                                  leftExpr: SparkExpr,
                                  rightExpr: SparkExpr)
  extends SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }

  /**
    * The computation of addition bases on double type
    */
  override def execute(exprName: String,
                       leftChild: Any,
                       rightExpr: Any): Double = {
    (leftChild, rightExpr) match {
      case (l: Number, r: Number) => l.doubleValue() + r.doubleValue()
      case (l: Number, r) => l.doubleValue() + r.toString.toDouble
      case (l, r: Number) => l.toString.toDouble + r.doubleValue()
    }
  }
}


object SparkAdd {
  def apply(@transient add: E_Add,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkAdd = new SparkAdd(add, leftExpr, rightExpr)
}