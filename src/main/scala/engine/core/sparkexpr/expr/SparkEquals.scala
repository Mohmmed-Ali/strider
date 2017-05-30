package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.E_Equals

/**
  * Created by xiangnanren on 19/05/2017.
  */


/**
  * For the comparison of two expression, currently we only consider
  * the primitive types (AnyVal), String and BigInteger. I.e., this allows to
  * simplify the comparison by directly using "==".
  *
  */
class SparkEquals(@transient val equals: E_Equals,
                  leftExpr: SparkExpr,
                  rightExpr: SparkExpr) extends
  SparkExpr2[SparkExpr, SparkExpr](leftExpr, rightExpr) {

  override def execute(exprName: String,
                       leftChild: Any,
                       rightChild: Any): Boolean = leftChild == rightChild


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}


object SparkEquals {
  def apply(equals: E_Equals,
            leftExpr: SparkExpr,
            rightExpr: SparkExpr): SparkEquals = new SparkEquals(equals, leftExpr, rightExpr)
}