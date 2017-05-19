package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_LogicalAnd

/**
  * Created by xiangnanren on 18/05/2017.
  */
class SparkAnd (val expr: E_LogicalAnd,
                leftExpr: SparkExpr,
                rightExpr: SparkExpr)
  extends SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr){

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }

}


object SparkAnd {
  def apply(expr: E_LogicalAnd,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkAnd = new SparkAnd(expr, leftExpr, rightExpr)
}