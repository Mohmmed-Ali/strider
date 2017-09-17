package engine.core.sparkop.compiler

import engine.core.sparkop.op._

/**
  * Created by xiangnanren on 07/07/16.
  */
trait SparkOpVisitor {

  def visit(sparkBGP: SparkBGP): Unit

  def visit(sparkFilter: SparkFilter): Unit

  def visit(sparkExtend: SparkExtend): Unit

  def visit(sparkGroup: SparkGroup): Unit

  def visit(sparkJoin: SparkJoin): Unit

  def visit(sparkLeftJoin: SparkLeftJoin): Unit

  def visit(sparkSequence: SparkSequence): Unit

  def visit(sparkUnion: SparkUnion): Unit

  def visit(sparkDistinct: SparkDistinct): Unit

  def visit(sparkProject: SparkProjection): Unit

  //  def visit(sparkReduced: SparkReduced)
  //  def visit(sparkOrder: SparkOrder)
  //  def visit(sparkSlice: SparkSlice)
}
