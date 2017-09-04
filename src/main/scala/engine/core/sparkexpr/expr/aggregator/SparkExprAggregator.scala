package engine.core.sparkexpr.expr.aggregator

import engine.core.sparkexpr.compiler.SparkExprVisitor
import engine.core.sparkexpr.expr.SparkExpr
import org.apache.jena.sparql.expr.ExprAggregator

/**
  * Created by xiangnanren on 04/09/2017.
  */
class SparkExprAggregator(@transient val expr: ExprAggregator) extends SparkExpr {



  override def visit(sparkExprVisitor: SparkExprVisitor): Unit =  {
    sparkExprVisitor.visit(this)
  }


}

object SparkExprAggregator {
  def apply(@transient expr: ExprAggregator): SparkExprAggregator =
    new SparkExprAggregator(expr)
}