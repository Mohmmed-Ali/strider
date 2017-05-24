package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.executor.SparkExprExecutor
import engine.core.sparkexpr.expr.SparkExpr
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.spark.sql.functions.udf


/**
  * Created by xiangnanren on 07/07/16.
  */

class SparkFilter(val opFilter: OpFilter,
                  subOp: SparkOp) extends
  SparkOp1(subOp: SparkOp) {
  val expr = transform(opFilter)

  def computeExpr(expr: SparkExpr) = udf(
    (arg: Any) => {
      SparkExprExecutor(arg).execute(expr)
    }
  )

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {


    null
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }

  private def transform(opFilter: OpFilter): SparkExpr = {
    val expr = opFilter.getExprs.iterator.next()

    (new SparkExprTransformer).transform(expr)
  }
}


object SparkFilter {
  def apply(opFilter: OpFilter,
            subOp: SparkOp): SparkFilter = new SparkFilter(opFilter, subOp)
}
