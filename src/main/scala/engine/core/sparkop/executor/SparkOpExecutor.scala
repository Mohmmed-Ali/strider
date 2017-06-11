package engine.core.sparkop.executor

import engine.core.sparkop.compiler.{SparkOpVisitorBase, SparkOpWalker}
import engine.core.sparkop.op._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * Created by xiangnanren on 13/07/16.
  */
class SparkOpExecutor(inputDF: DataFrame) extends
  SparkOpVisitorBase {
  private[this] val stack = new mutable.Stack[SparkOpRes]

  def execute(op: SparkOp): SparkOpRes = {
    SparkOpWalker(this).walkBottomUp(op)
    stack.pop()
  }

  override def visit(sparkBGP: SparkBGP): Unit = {
    stack.push(sparkBGP.execute(
      sparkBGP.opName,
      inputDF))
  }

  override def visit(sparkDistinct: SparkDistinct): Unit = {
    stack.push(sparkDistinct.execute(
      sparkDistinct.opName,
      stack.pop()))
  }

  override def visit(sparkFilter: SparkFilter): Unit = {
    stack.push(sparkFilter.execute(
      sparkFilter.opName,
      stack.pop()))
  }

  override def visit(sparkGroup: SparkGroup): Unit = {

  }

  override def visit(sparkJoin: SparkJoin): Unit = {
    lazy val rightSubOp = stack.pop()
    lazy val leftSubOp = stack.pop()

    stack.push(sparkJoin.execute(
      sparkJoin.opName,
      leftSubOp,
      rightSubOp))
  }

  override def visit(sparkLeftJoin: SparkLeftJoin): Unit = {
    lazy val rightSubOp = stack.pop()
    lazy val leftSubOp = stack.pop()

    stack.push(sparkLeftJoin.execute(
      sparkLeftJoin.opName,
      leftSubOp,
      rightSubOp))
  }

  override def visit(sparkProject: SparkProjection): Unit = {
    stack.push(sparkProject.execute(
      sparkProject.opName,
      stack.pop()))
  }

  override def visit(sparkSequence: SparkSequence): Unit = {}

  override def visit(sparkUnion: SparkUnion): Unit = {
    lazy val rightSubOp = stack.pop()
    lazy val leftSubOp = stack.pop()

    stack.push(sparkUnion.execute(
      sparkUnion.opName,
      leftSubOp,
      rightSubOp))
  }
}


object SparkOpExecutor {
  def apply(inputDF: DataFrame): SparkOpExecutor = new SparkOpExecutor(inputDF)
}