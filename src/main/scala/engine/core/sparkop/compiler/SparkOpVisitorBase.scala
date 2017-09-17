package engine.core.sparkop.compiler

import engine.core.sparkop.op._
import org.apache.log4j.LogManager

/**
  * Created by xiangnanren on 07/07/16.
  */
class SparkOpVisitorBase extends SparkOpVisitor {

  @transient
  lazy val log = LogManager.getLogger(SparkOpVisitorBase.getClass)

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkBGP: SparkBGP): Unit = {
    log.error("BGP is not supported.")
    throw new UnsupportedOperationException("BGP is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkDistinct: SparkDistinct): Unit = {
    log.error("DISTINCT is not supported.")
    throw new UnsupportedOperationException("DISTINCT is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkExtend: SparkExtend): Unit = {
    log.error("EXTEND is not supported")
    throw new UnsupportedOperationException("EXTEND is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkFilter: SparkFilter): Unit = {
    log.error("FILTER is not supported.")
    throw new UnsupportedOperationException("FILTER is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkGroup: SparkGroup): Unit = {
    log.error("GROUP By is not supported.")
    throw new UnsupportedOperationException("GROUP By is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkJoin: SparkJoin): Unit = {
    log.error("JOIN is not supported.")
    throw new UnsupportedOperationException("JOIN is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkSequence: SparkSequence): Unit = {
    log.error("SEQUENCE is not supported.")
    throw new UnsupportedOperationException("SEQUENCE is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkLeftJoin: SparkLeftJoin): Unit = {
    log.error("LEFT-JOIN is not supported yet")
    throw new UnsupportedOperationException("LEFT-JOIN is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sqlProject: SparkProjection): Unit = {
    log.error("PROJECT is not supported.")
    throw new UnsupportedOperationException("PROJECT is not supported.")
  }

  @throws(classOf[UnsupportedOperationException])
  override def visit(sparkUnion: SparkUnion): Unit = {
    log.error("UNION is not supported yet")
    throw new UnsupportedOperationException("UNION is not supported.")
  }

}

object SparkOpVisitorBase {
  def apply: SparkOpVisitorBase = new SparkOpVisitorBase()
}
