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
  /**
    * [[groupVars]] The key variables for group operator
    *
    * [[aggs]] The list of aggregators contained in current group operator
    *
    * [[transformedAggs]] Transformed expressions of aggregation
    *
    */
  val groupVars = opGroup.getGroupVars.getVars.map(x => x.getVarName)
  val aggs = opGroup.getAggregators.toList
  println(s"check in group  ${aggs(0).getExpr}")

  val transformedAggs = try {
    aggs.map(agg => (new SparkExprTransformer).transform(agg))
  } catch {
    case ex: Exception =>
      throw NullExprException("The expression in " + this.opName + " is null")
  }



  def computeGroup(inputDF: DataFrame): DataFrame = {
    val groupedDF = if (groupVars.length == 1)
      inputDF.groupBy(groupVars.head)
    else inputDF.groupBy(groupVars.head, groupVars.tail:_*)

//    df.agg(MyMax(df.col("age")).as("max_age")).show(20,false:Boolean)

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