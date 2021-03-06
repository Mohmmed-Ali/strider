package engine.core.sparkop.op

import engine.core.sparkexpr.executor.ExprUDF
import engine.core.sparkexpr.expr.{NullExprException, ExprElementOutOfBoundException}
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpFilter
import org.apache.jena.sparql.expr.Expr

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 07/07/16.
  */

class SparkFilter(val opFilter: OpFilter,
                  subOp: SparkOp) extends
  SparkOp1(subOp: SparkOp) {
  @throws(classOf[NullExprException])
  /**
    * [[expr]] The used expression in filter operator
    *
    * [[columnNames]] refers to the names of multiple columns
    * which are involved in filter.
    *
    * [[columnName]] refers to the only column to be performed in filter.
    *
    * Since current Spark SQL user defined function for filter
    * operation only allows to process a single column for once,
    * the implementation of SparkFilter operator only considers the case
    * which filter operation requires a single column by default.
    *
    */
  private val expr = opFilter.getExprs.iterator.next()
  private val columnNames = setColumnName(expr)
  private val columnName = setColumnName(expr).head
  val transformedExpr = transformExpr(expr, this.opName)
  val filterUDF = ExprUDF.BooleanTypeUDF(columnNames, transformedExpr)

  /**
    * Set the name of output (filtered) column.
    * Limited by Spark, the filter UDF only allows a single input variable.
    */
  @throws(classOf[ExprElementOutOfBoundException])
  def setColumnName(expr: Expr): Vector[String] = {
    expr.getVarsMentioned.size match {
      case size if size > 1 =>
        throw ExprElementOutOfBoundException(
          "The number of variable in an expression should be less than 1")
      case size if size == 1 => expr.getVarsMentioned.map(v => v.getVarName).toVector
    }
  }


  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    val df = child.result
    SparkOpRes(df.filter(filterUDF(df(s"$columnName"))))
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }

}


object SparkFilter {
  def apply(opFilter: OpFilter,
            subOp: SparkOp): SparkFilter = new SparkFilter(opFilter, subOp)
}
