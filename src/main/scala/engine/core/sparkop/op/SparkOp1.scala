package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.expr.{NullExprException, SparkExpr}
import org.apache.jena.sparql.expr.Expr

/**
  * Created by xiangnanren on 07/07/16.
  */
abstract class SparkOp1[T <: SparkOp](val subOp: T) extends
  SparkOpBase {

  def execute(opName: String,
              child: SparkOpRes): SparkOpRes

  protected def transformExpr(expr: Expr,
                              opName: String): SparkExpr = {
    try {
      (new SparkExprTransformer).
        transform(expr)
    } catch {
      case _: Exception =>
        throw NullExprException("The expression in" + opName + "is null")
    }
  }
}
