package engine.core.sparkop.compiler

import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.algebra.{Op, OpVisitor, OpVisitorByType}
import org.apache.log4j.LogManager

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 07/
  * 07/16.
  */
class OriginalOpWalker(val visitor: OpVisitor)
  extends OpVisitorByType {
  @transient
  private[this] lazy val log = LogManager.
    getLogger(OriginalOpWalker.getClass)

  def walkBottomUp(op: Op): Unit = {
    log.debug("walkBottomUp called.")
    op.visit(this)
  }

  override def visit0(op: Op0): Unit = {
    log.debug("visit0 called.")
    op.visit(visitor)
  }

  override def visit1(op: Op1): Unit = {
    log.debug("visit1 called.")
    if (Option(op.getSubOp).nonEmpty)
      op.getSubOp.visit(this)
    op.visit(visitor)
  }

  override def visit2(op: Op2): Unit = {
    log.debug("visit2 called.")
    if (Option(op.getLeft).nonEmpty)
      op.getLeft.visit(this)
    if (Option(op.getRight).nonEmpty)
      op.getRight.visit(this)
    op.visit(visitor)
  }

  override def visitN(op: OpN): Unit = {
    log.debug("visitN called.")
    val iter: Iterator[Op] = op.iterator.toIterator
    while (iter.hasNext) {
      iter.next().visit(this)
    }
    op.visit(visitor)
  }

  override def visitExt(op: OpExt): Unit = {
    op.visit(visitor)
  }

  override def visitFilter(op: OpFilter): Unit = {
    if (Option(op.getSubOp).nonEmpty)
      op.getSubOp.visit(this)
    op.visit(visitor)
  }

  override def visitLeftJoin(op: OpLeftJoin): Unit = {
    if (Option(op.getLeft).nonEmpty)
      op.getLeft.visit(this)
    if (Option(op.getRight).nonEmpty)
      op.getRight.visit(this)
    op.visit(visitor)
  }
}


object OriginalOpWalker {
  def apply(visitor: OpVisitor):
  OriginalOpWalker = new OriginalOpWalker(visitor)
}