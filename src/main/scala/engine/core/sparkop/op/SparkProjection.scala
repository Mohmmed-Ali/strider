package engine.core.sparkop.op

import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpProject
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 08/07/16.
  */

class SparkProjection(val opProject: OpProject,
                      subOp: SparkOp) extends
  SparkOpModifier(subOp: SparkOp) {
  val varList = opProject.getVars.toList
  val schema = varList.map(x => x.getVarName)

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    SparkOpRes(computeProject(child.result))
  }

  private def computeProject(inputDF: DataFrame): DataFrame =
    schema.length match {
      case 1 => inputDF.select(schema.head)
      case _ => inputDF.select(schema.head, schema.tail: _*)
    }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}

object SparkProjection {
  def apply(opProject: OpProject,
            subOp: SparkOp): SparkProjection = new SparkProjection(opProject, subOp)
}