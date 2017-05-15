package engine.core.optimizer.epgenerator

import engine.core.optimizer.ExecutionPlanType.{AdaptiveGreedy, StaticGreedy}
import engine.core.optimizer.ucg.{BGPEdge, BGPGraph, BGPNode, UCGraph}
import engine.core.optimizer.{ExecutionPlanType, InvalidEPException}

import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 06/12/2016.
  */
class GreedyEPGenerator[T <: ExecutionPlanType](epType: T)
  extends EPGenerator {

  override def generate(ucg: UCGraph): List[BGPGraph] = {
    val ucgNodes: List[BGPNode] = ucg.ucgNodes
    ucgNodes.foreach(node => node.visited = false)

    val ep = new ArrayBuffer[BGPGraph]
    val nodeCandidates = new ArrayBuffer[BGPNode]

    val initNode = minUCGNode(ucgNodes, epType)
    ep.append(initNode)
    nodeCandidates.append(initNode)
    markVisited(initNode)

    while (ucgNodes.exists(node => !node.visited)) {
      val currentMinNode = createConnectedNodes(nodeCandidates, ucgNodes).
        reduce((node1, node2) => node1.minNode(node2, epType))

      ep.append(currentMinNode)
      ep.append(getConnectedEdge(nodeCandidates,currentMinNode))

      nodeCandidates.append(currentMinNode)
      markVisited(currentMinNode)
    }
    ep.toList
  }


  private[this] def createConnectedNodes(nodeCandidates: Seq[BGPNode],
                                         ucgNodes: Seq[BGPNode]): mutable.Set[BGPNode] = {
    val set = mutable.Set[BGPNode]()

    for (i <- nodeCandidates;
         j <- ucgNodes
         if graphConnection(i, j) && !j.visited
    ){
      set.add(j)
    }
    set
  }


  private[this] def getConnectedEdge(nodeCandidates: Seq[BGPNode],
                 currentMinNode:BGPNode): BGPEdge = {
    val node = nodeCandidates.
      find(_node => graphConnection(_node, currentMinNode)).get

    BGPEdge(node, currentMinNode)
  }

  private[this] def markVisited(node: BGPNode): Unit = node.visited = true

}


object GreedyEPGenerator {
  def apply[T <: ExecutionPlanType](epType: T) = epType match {

    case StaticGreedy => new GreedyEPGenerator(StaticGreedy)

    case AdaptiveGreedy => new GreedyEPGenerator(AdaptiveGreedy)

    case _ => throw InvalidEPException("Invalid execution plan type, " +
      "the type should be either HeuByNode or StatByNode, " +
      "execution plan generator initialize failed")
  }
}