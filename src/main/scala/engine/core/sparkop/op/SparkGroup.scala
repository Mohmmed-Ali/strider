package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.expr.NullExprException
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpGroup
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 02/08/16.
  */
class SparkGroup(val opGroup: OpGroup,
                 subOp: SparkOp) extends SparkOp1(subOp: SparkOp) {
  val groupVars = opGroup.
    getGroupVars.
    getVars.
    toList.
    map(x => x.getVarName)

  val agg = opGroup.getAggregators.toList.iterator.next()
  val transformedExpr = try {
    (new SparkExprTransformer).transform(agg)
  } catch {
    case ex: Exception =>
      throw NullExprException("The expression in" + this.opName + "is null")
  }




  def computeGroup(inputDF: DataFrame): DataFrame = {
    null
  }

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    null
  }

  override def visit(sqlOpVisitor: SparkOpVisitor): Unit = {
    sqlOpVisitor.visit(this)
  }
}


object SparkGroup {
  def apply(opGroup: OpGroup,
            subOp: SparkOp): SparkGroup = new SparkGroup(opGroup, subOp)
}