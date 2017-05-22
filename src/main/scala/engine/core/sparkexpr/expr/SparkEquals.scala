package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_Equals

/**
  * Created by xiangnanren on 19/05/2017.
  */
class SparkEquals(val equals: E_Equals,
                  leftExpr: SparkExpr,
                  rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {



  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }

}


object SparkEquals {
  def apply(equals: E_Equals,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkEquals = new SparkEquals(equals, leftExpr, rightExpr)
}