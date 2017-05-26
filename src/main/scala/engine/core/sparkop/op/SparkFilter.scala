package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.executor.ExprUDF
import engine.core.sparkexpr.expr.{NullExprException, VarOutOfBoundException}
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
  val expr = opFilter.getExprs.iterator.next()
  val columnName = setColumnName(expr)

  @throws(classOf[NullExprException])
  val transformedExpr = try {
    (new SparkExprTransformer).transform(expr)
  } catch {
    case ex: Exception =>
      throw NullExprException("The expression in" + this.opName + "is null")
  }

  val filterUDF = ExprUDF.UDFWithBoolean(transformedExpr)

  @throws(classOf[VarOutOfBoundException])
  def setColumnName(expr: Expr): String = {
    expr.getVarsMentioned.size match {
      case size if size > 1 =>
        throw VarOutOfBoundException(
          "The number of variable in an expression should be less than 1")
      case size if size == 1  => expr.getVarsMentioned.head.getVarName
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
