package engine.core.sparkop.op

import engine.core.label.SparkOpLabels
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpLeftJoin
import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 08/07/16.
  */
class SparkLeftJoin(val opLeftJoin: OpLeftJoin,
                    leftOp: SparkOp,
                    rightOp: SparkOp) extends
  SparkOp2(leftOp, rightOp) {

  private def computeLefJoin(leftDF: DataFrame,
                             rightDF: DataFrame): DataFrame = {
    val leftColName = leftDF.columns
    val rightColName = rightDF.columns

    val f_JoinKey = (leftColName: Array[String],
                     rightColName: Array[String]) => {
      for (i <- rightColName if leftColName contains i) yield i
    }

    val leftJoinResult = leftDF.join(
      rightDF,
      f_JoinKey(leftColName, rightColName),
      SparkOpLabels.LEFT_JOIN
    )

    leftJoinResult
  }

  override def execute(opName: String,
                       leftChild: SparkOpRes,
                       rightChild: SparkOpRes): SparkOpRes = {
    SparkOpRes(
      computeLefJoin(
        leftChild.result,
        rightChild.result))
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}


object SparkLeftJoin {
  def apply(opLeftJoin: OpLeftJoin,
            leftOp: SparkOp,
            rightOp: SparkOp): SparkLeftJoin = new SparkLeftJoin(opLeftJoin, leftOp, rightOp)
}