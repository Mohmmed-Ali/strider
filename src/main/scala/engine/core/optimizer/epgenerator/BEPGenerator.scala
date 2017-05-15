package engine.core.optimizer.epgenerator

import engine.core.optimizer.EmptyUCGException
import engine.core.optimizer.ucg.{BGPEdge, BGPGraph, BGPNode, UCGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 06/12/2016.
  */
object BEPGenerator extends EPGenerator {

  /**
    * Generate the basic plan for the inner join of triple patterns
    * in BGP operator. The basic plan is a primitive routing policy
    * which only tries to avoid cartesian product among each triple pattern
    *
    * @param g : input UCG
    * @return : basic execution plan avoids cartesian product
    */
  override def generate(g: UCGraph): List[BGPGraph] = {
    val ucgEdges: List[BGPEdge] = g.ucgEdges
    val orderNodes: ArrayBuffer[BGPNode] = new mutable.ArrayBuffer[BGPNode]
    val nodeCandidates: mutable.Set[BGPNode] = mutable.Set[BGPNode]()

    nodeCandidates.add(ucgEdges.head.node1)

    ucgEdges.nonEmpty match {

      case true => ucgEdges.foreach { edge =>

        if (setContains(edge.node1, nodeCandidates)) {
          nodeCandidates.add(edge.node2)
          if (!seqContains(edge.node2, orderNodes))
            orderNodes.append(edge.node2)
        }

        if (setContains(edge.node2, nodeCandidates)) {
          nodeCandidates.add(edge.node1)
          if (!seqContains(edge.node1, orderNodes))
            orderNodes.append(edge.node1)
        }
      }

      case false => throw EmptyUCGException("The query  ucgEdges is empty")
    }
    orderNodes.toList
  }
}
