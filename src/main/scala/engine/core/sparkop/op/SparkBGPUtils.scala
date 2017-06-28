package engine.core.sparkop.op

import engine.core.label.SparkOpLabels
import engine.core.optimizer.ucg.{BGPEdge, BGPGraph, BGPNode}
import org.apache.jena.graph
import org.apache.spark.sql._

import scala.collection.mutable

/**
  * Created by xiangnanren on 09/11/2016.
  */
trait SparkBGPUtils extends BGPUtils{

  protected def computeDFMap(triples: List[graph.Triple],
                             inputDF: DataFrame):
  Map[graph.Triple, DataFrame] = {
    triples.map(triple =>
      triple ->
        computeTriplePattern(triple, inputDF)).toMap
  }

  protected def computeEP(ep: Seq[BGPGraph],
                          dfMap: Map[graph.Triple, DataFrame]): DataFrame = {
    val stack = new mutable.Stack[DataFrame]

    for (g <- ep) {
      g match {
        case g: BGPNode =>
          stack.push(dfMap(g.triple))

        case g: BGPEdge =>
          val rightChild = stack.pop()
          val leftChild = stack.pop()

          val res = leftChild.join(
            rightChild,
            g.joinKey,
            SparkOpLabels.BGP_INNER_JOIN)
          stack.push(res)

          leftChild.unpersist(true)
          rightChild.unpersist(true)
      }
    }
    stack.pop()
  }

  protected def getEPAsString(ep: List[BGPGraph]): String = {
    ep.map(_.toString).
      reduceLeft((x, y) => x + "\n" + y)
  }

  protected def getEPInfo(ep: List[BGPGraph]): String = {
    ep.map(_.getInfo).
      reduceLeft((x, y) => x + "\n" + y)
  }
}
