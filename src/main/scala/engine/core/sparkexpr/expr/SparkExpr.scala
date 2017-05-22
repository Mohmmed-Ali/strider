package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor

/**
  * Created by xiangnanren on 03/05/2017.
  */
trait SparkExpr {
  var exprName: String = "SPARK_EXPR"



  def visit(sparkExprVisitor: SparkExprVisitor): Unit


}
