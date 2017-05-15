package engine.core.sparkop.compiler

import engine.core.sparkop.op._

/**
  * Created by xiangnanren on 07/07/16.
  */
abstract class SparkOpVisitorByType extends SparkOpVisitor {

  def visit0(op: SparkOp0)

  def visit1(op: SparkOp1[SparkOp])

  def visit2(op: SparkOp2[SparkOp, SparkOp])

  def visitN(op: SparkOpN)


  /**
    * Define basic visit methods as final,
    * the override is not permitted.
    */
  override final def visit(sparkBGP: SparkBGP): Unit = {
    visit0(sparkBGP)
  }

  override final def visit(sparkDistinct: SparkDistinct): Unit = {
    visit1(sparkDistinct)
  }

  override final def visit(sparkFilter: SparkFilter): Unit = {
    visit1(sparkFilter)
  }

  override final def visit(sparkGroup: SparkGroup): Unit = {
    visit1(sparkGroup)
  }

  override final def visit(sparkJoin: SparkJoin): Unit = {
    visit2(sparkJoin)
  }

  override final def visit(sparkSequence: SparkSequence): Unit = {
    visitN(sparkSequence)
  }

  override final def visit(sparkLeftJoin: SparkLeftJoin): Unit = {
    visit2(sparkLeftJoin)
  }

  override final def visit(sparkUnion: SparkUnion): Unit = {
    visit2(sparkUnion)
  }

  override final def visit(sparkProject: SparkProjection): Unit = {
    visit1(sparkProject)
  }

}
