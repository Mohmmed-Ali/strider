package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.{Expr, ExprAggregator}

import scala.collection.JavaConversions._

/**
  * Created by xiangnanren on 04/09/2017.
  */
private[sparkexpr] class SparkExprAggregator(@transient val expr: ExprAggregator)
  extends SparkExpr {

  println(s"visit SparkExprAggregator ${expr.getAggregator.getExprList.getList.toList}")
  println(s"visit SparkExprAggregator ${expr.getAggregator.getExprList.getList.toList.length}")

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

private[sparkexpr] object SparkExprAggregator {
  def apply(@transient expr: ExprAggregator): SparkExprAggregator =
    new SparkExprAggregator(expr)


  def getExprs(expr: ExprAggregator): Expr = {
    expr.getAggregator.getExprList.getList.toList match {
      case l if l.length == 1 => l.head
      case _ => throw ExprElementOutOfBoundException(
        "There should be only one expression in each aggregator")
    }
  }

//  def computeArithmeticExpr()

}