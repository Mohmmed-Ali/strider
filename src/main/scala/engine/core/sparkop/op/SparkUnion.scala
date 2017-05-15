package engine.core.sparkop.op

import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpUnion
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

/**
  * Created by xiangnanren on 08/07/16.
  */
class SparkUnion(val opUnion: OpUnion,
                 leftOp: SparkOp,
                 rightOp: SparkOp) extends
  SparkOp2[SparkOp, SparkOp](leftOp, rightOp) {
  @tailrec
  private def convertSchema(toAddColLeft: List[String],
                            accumDF: DataFrame): DataFrame = {
    toAddColLeft match {
      case col :: tail => convertSchema(tail,
        accumDF.withColumn(col, lit(null)))
      case Nil => accumDF
    }
  }

  private def computeUnion(leftDF: DataFrame,
                           rightDF: DataFrame): DataFrame = {
    val toAddColLeft = rightDF.columns.
      filterNot(leftDF.columns.contains(_))
    val toAddColRight = leftDF.columns.
      filterNot(rightDF.columns.contains(_))

    val commonSchema = leftDF.columns ++ toAddColLeft

    val leftDF1 = commonSchema.length match {
      case 1 => convertSchema(toAddColLeft.toList, leftDF).
        select(commonSchema.head)
      case _ => convertSchema(toAddColLeft.toList, leftDF).
        select(commonSchema.head, commonSchema.tail: _*)
    }

    val rightDF1 = commonSchema.length match {
      case 1 => convertSchema(toAddColRight.toList, rightDF).
        select(commonSchema.head)
      case _ => convertSchema(toAddColRight.toList, rightDF).
        select(commonSchema.head, commonSchema.tail: _*)
    }

    leftDF1.union(rightDF1)
  }

  override def execute(opName: String,
                       leftChild: SparkOpRes,
                       rightChild: SparkOpRes): SparkOpRes = {
    SparkOpRes(computeUnion(
      leftChild.result,
      rightChild.result))
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}


object SparkUnion {
  def apply(opUnion: OpUnion,
            leftOp: SparkOp,
            rightOp: SparkOp): SparkUnion = new SparkUnion(opUnion, leftOp, rightOp)
}