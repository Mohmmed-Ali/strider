package engine.core.sparkop.compiler

import engine.core.label.SparkOpLabels
import engine.core.sparkop.op._
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.algebra.{Op, OpVisitorBase}
import org.apache.log4j.LogManager

import scala.collection.JavaConversions._
import scala.collection.mutable


/**
  * Created by xiangnanren on 07/07/16.
  */
class OpTransformer extends OpVisitorBase with Serializable {
  @transient
  private[this] val log = LogManager.
    getLogger(OpTransformer.getClass)

  private val stack = new mutable.Stack[SparkOp]
  private var opID = -1

  /** Transform the Jena algebra tree into
    * SPARK operators tree, and return its root.
    *
    * @param op the input Jena algebra root node
    */
  def transform(op: Op): SparkOp = {
    log.info(s"op: $op")

    SparqlOpWalker(this).walkBottomUp(op)
    stack.pop()
  }

  override def visit(opBGP: OpBGP): Unit = {
    opID += 1
    log.debug(s"opID: $opID, opBGP: $opBGP")

    val sparkBGP = SparkBGP(opBGP, opBGP.getPattern.getList.toList)
    sparkBGP.opName = SparkOpLabels.BGP_NAME + opID
    stack.push(sparkBGP)
  }

  override def visit(opDistinct: OpDistinct): Unit = {
    val subOp = stack.pop()

    opID += 1
    log.debug(s"opID: $opID, opDistinct: $opDistinct")
    log.debug(s"subOp: $subOp")

    val sparkDistinct = SparkDistinct(opDistinct, subOp)
    sparkDistinct.opName = SparkOpLabels.DISTINCT_NAME + opID
    stack.push(sparkDistinct)
  }

  override def visit(opFilter: OpFilter): Unit = {
    val subOp: SparkOp = stack.pop()

    opID += 1
    log.debug(s"opID: $opID, opFilter: $opFilter")

    val sparkFilter = SparkFilter(opFilter, subOp)
    sparkFilter.opName = SparkOpLabels.FILTER_NAME + opID
    stack.push(sparkFilter)
  }

  override def visit(opGroup: OpGroup): Unit = {
    val subOp: SparkOp = stack.pop()

    opID += 1
    log.debug(s"opID: $opID, opGroup: $opGroup")

    val sparkGroup = SparkGroup(opGroup, subOp)
    sparkGroup.opName = SparkOpLabels.GROUP_NAME + opID
    stack.push(sparkGroup)
  }

  override def visit(opJoin: OpJoin): Unit = {
    val rightOp: SparkOp = stack.pop()
    val leftOp: SparkOp = stack.pop()

    opID += 1
    log.debug(s"rightOp: $rightOp")
    log.debug(s"leftOp: $leftOp")
    log.debug(s"opID: $opID, opJoin: $opJoin")

    val sparkJoin = SparkJoin(opJoin, leftOp, rightOp)
    sparkJoin.opName = SparkOpLabels.JOIN_NAME + opID

    stack.push(sparkJoin)
  }

  override def visit(opLeftJoin: OpLeftJoin): Unit = {
    // The order of retrieval the elements from stack is important
    val leftOp: SparkOp = stack.pop()
    val rightOp: SparkOp = stack.pop()

    opID += 1
    log.debug(s"rightOp: $rightOp")
    log.debug(s"leftOp: $leftOp")
    log.debug(s"opID: $opID , opLeftJoin: $opLeftJoin")

    val sparkLeftJoin = SparkLeftJoin(opLeftJoin, leftOp, rightOp)
    sparkLeftJoin.opName = SparkOpLabels.LEFT_JOIN_NAME + opID

    stack.push(sparkLeftJoin)
  }

  override def visit(opUnion: OpUnion): Unit = {
    val rightOp: SparkOp = stack.pop()
    val leftOp: SparkOp = stack.pop()
    opID += 1

    log.debug(s"rightOp: $rightOp")
    log.debug(s"leftOp: $leftOp")
    log.debug(s"opID: $opID , opUnion: $opUnion")

    val sparkUnion = SparkUnion(opUnion, leftOp, rightOp)
    sparkUnion.opName = SparkOpLabels.UNION_NAME + opID
    stack.push(sparkUnion)
  }

  override def visit(opProject: OpProject): Unit = {
    val subOp: SparkOp = stack.pop()
    opID += 1

    log.debug(s"subOp: $subOp")
    log.debug(s"opID: $opID, opProject: $opProject")

    val sparkProject = SparkProjection(opProject, subOp)
    sparkProject.opName = SparkOpLabels.PROJECT_NAME + opID
    stack.push(sparkProject)
  }

}

object OpTransformer {
  def apply: OpTransformer = new OpTransformer()
}

