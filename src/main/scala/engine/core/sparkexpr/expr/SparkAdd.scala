package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_Add

/**
  * Created by xiangnanren on 03/05/2017.
  */
class SparkAdd(val add: E_Add,
               leftExpr: SparkExpr,
               rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }

  override def execute(exprName: String, leftChild: ExprResMapping, rightExpr: ExprResMapping): ExprResMapping = ???
}


object SparkAdd {
  def apply(add: E_Add,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkAdd = new SparkAdd(add, leftExpr, rightExpr)
}