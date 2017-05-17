package engine.core.sparkop.op

import engine.core.label.SparkOpLabels
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpJoin
import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 08/07/16.
  */
class SparkJoin(val opJoin: OpJoin,
                leftOp: SparkOp,
                rightOp: SparkOp) extends
  SparkOp2[SparkOp, SparkOp](leftOp, rightOp) {

  override def execute(opName: String,
                       leftChild: SparkOpRes,
                       rightChild: SparkOpRes): SparkOpRes = {
    SparkOpRes(computeJoin(
      leftChild.result,
      rightChild.result
    ))
  }

  private def computeJoin(leftDF: DataFrame,
                          rightDF: DataFrame): DataFrame = {
    val leftColName = leftDF.columns
    val rightColName = rightDF.columns

    val f_JoinKey = (leftColName: Array[String],
                     rightColName: Array[String]) => {
      for (i <- rightColName if leftColName contains i) yield i
    }

    val joinResult = leftDF.join(
      rightDF,
      f_JoinKey(leftColName, rightColName),
      SparkOpLabels.JOIN
    )

    joinResult
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}


object SparkJoin {
  def apply(opJoin: OpJoin,
            leftOp: SparkOp,
            rightOp: SparkOp): SparkJoin = new SparkJoin(opJoin, leftOp, rightOp)
}