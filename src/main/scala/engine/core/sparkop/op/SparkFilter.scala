package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.executor.SparkExprExecutor
import engine.core.sparkexpr.expr.{NullExprException, SparkExpr, VarOutOfBoundException}
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.sql.functions.udf


/**
  * Created by xiangnanren on 07/07/16.
  */

class SparkFilter(val opFilter: OpFilter,
                  subOp: SparkOp) extends
  SparkOp1(subOp: SparkOp) {
  val expr = opFilter.getExprs.iterator.next()
  val columnName = setColumnName(expr)

  @throws(classOf[NullExprException])
  val transformedExpr = try {
    (new SparkExprTransformer).transform(expr)
  } catch {
    case ex: Exception =>
      throw NullExprException("The expression in" + this.opName + "is null")
  }

  @throws(classOf[VarOutOfBoundException])
  def setColumnName(expr: Expr): String = {
    expr.getExprVar.getVarNamesMentioned.size match {
      case size if size > 1 =>
        throw VarOutOfBoundException(
          "The number of variable in an expression should be less than 1")
      case size if size == 1  => expr.getVarName
    }
  }

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
