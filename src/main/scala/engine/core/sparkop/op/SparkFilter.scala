package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.Expr

/**
  * Created by xiangnanren on 07/07/16.
  */

class SparkFilter(val opFilter: OpFilter,
                  subOp: SparkOp) extends
  SparkOp1(subOp: SparkOp) {
  val expr = transform(opFilter)

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    SparkOpRes(child.result.filter(expr))
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }

  private def transform(opFilter: OpFilter): String = {
    val expr = opFilter.getExprs.iterator.next()

    (new SparkExprTransformer).transform(expr)
    ""
  }
}


object SparkFilter {
  def apply(opFilter: OpFilter,
            subOp: SparkOp): SparkFilter = new SparkFilter(opFilter, subOp)
}
