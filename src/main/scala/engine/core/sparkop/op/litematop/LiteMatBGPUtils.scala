package engine.core.sparkop.op.litematop

import engine.core.label.SparkOpLabels
import engine.core.optimizer.ucg.{BGPEdge, BGPGraph, BGPNode}
import engine.core.sparkop.op.BGPUtils
import org.apache.jena.graph
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by xiangnanren on 15/06/2017.
  */
trait LiteMatBGPUtils extends BGPUtils {

  def computeDFMapViaLiteMat(liteMatBGPNodeMapping: Map[graph.Triple, LiteMatBGPNode],
                             inputDF: DataFrame): Map[graph.Triple, DataFrame] =
    liteMatBGPNodeMapping.map(t =>
      t._1 -> t._2.compute(inputDF))

  /**
    * The method computes the results of current BGP based on
    * the optimized execution plan and the computed triple patterns by LiteMat
    *
    * @param ep    : optimized execution plan
    * @param dfMap : triples patterns which are computed by LiteMat
    */
  def computeEPViaLiteMat(ep: Seq[BGPGraph],
                          dfMap: Map[graph.Triple, DataFrame]): DataFrame = {
    val stack = new mutable.Stack[DataFrame]

    for (g <- ep) {
      g match {
        case _g: BGPNode => stack.push(dfMap(_g.triple))
        case _g: BGPEdge =>
          val rightChild = stack.pop()
          val leftChild = stack.pop()

          val res = leftChild.join(
            rightChild,
            _g.joinKey,
            SparkOpLabels.BGP_INNER_JOIN)
          stack.push(res)

          leftChild.unpersist(true)
          rightChild.unpersist(true)
      }
    }
    stack.pop()
  }

  protected def rename(node: BGPNode,
                       inputDF: DataFrame): DataFrame = {
    val tripleS = node.triple.getSubject
    val tripleP = node.triple.getPredicate
    val tripleO = node.triple.getObject

    val outputDF = (
      tripleS.isVariable,
      tripleP.isVariable,
      tripleO.isVariable
      ) match {
      case (true, false, false) =>
        inputDF.withColumnRenamed("sDefault", tripleS.getName)
      case (false, true, false) =>
        inputDF.withColumnRenamed("pDefault", tripleP.getName)
      case (false, false, true) =>
        inputDF.withColumnRenamed("oDefault", tripleO.getName)
      case (true, true, false) =>
        inputDF.withColumnRenamed("sDefault", tripleS.getName).
          withColumnRenamed("pDefault", tripleP.getName)
      case (true, false, true) =>
        inputDF.withColumnRenamed("sDefault", tripleS.getName).
          withColumnRenamed("oDefault", tripleO.getName)
      case (false, true, true) =>
        inputDF.withColumnRenamed("pDefault", tripleP.getName).
          withColumnRenamed("oDefault", tripleO.getName)
      case (true, true, true) =>
        inputDF.withColumnRenamed("sDefault", tripleS.getName).
          withColumnRenamed("pDefault", tripleP.getName).
          withColumnRenamed("oDefault", tripleO.getName)
      case (false, false, false) => inputDF
    }
    outputDF
  }

}

