package engine.core.optimizer.ucg

import engine.core.optimizer.BGPOptimizerHelper
import org.apache.jena.graph

/**
  * Created by xiangnanren on 27/10/2016.
  */

/**
  * Primary constructor of UCGraph accepts an argument of SparkBGP
  * This construction is used for global algebra reconstruction, i.e.,
  * when the whole query algebra needs to be reconstructed at runtime,
  * UCGraph should be initialized with its primary constructor.
  *
  * @param ucgNodes : initial BGP execution plan
  */
class UCGraph(val ucgNodes: List[BGPNode]) extends BGPOptimizerHelper {

  val ucgEdges: List[BGPEdge] = generateUCGEdges()
  this.ucgNodes.map(node =>
    node.setStarJoinNodes(this.ucgNodes))

  /**
    * Create the undirected connected graph for a given execution plan.
    * Only considering unbound node as join key
    *
    * @return UCGraph : a list of connected triple pattern ((tp1, tp2)...)
    */
  private def generateUCGEdges(): List[BGPEdge] = {

    val edgeSeq: IndexedSeq[BGPEdge] =
      for {
        i <- ucgNodes.indices
        j <- i + 1 until ucgNodes.length
        if graphConnection(ucgNodes(i), ucgNodes(j))
      } yield BGPEdge(ucgNodes(i), ucgNodes(j))

    edgeSeq.toList
  }


  /**
    * This method updates the statistic weight of
    * all BGPNodes weight in current UCG graph
    * @param nodeMapping: Map[graph.Triple, Long], graph.Triple: triple pattern,
    *                    Long: the statistic weight of the triple pattern
    */
  def updateWeight(nodeMapping: Map[graph.Triple, Long]): Unit = {
    // Update nodes weight
    ucgNodes.foreach(node => {
      node.statisticWeight = nodeMapping(node.triple)})
    // Update edge weight
    ucgEdges.foreach(edge => edge.updateEdgeStaticWeight())
  }

  def size(): Int = ucgEdges.length

  def showUCG(): Unit = ucgEdges.foreach(edge =>
    println(
      "Join pattern weight: " +
        edge.heuristicWeight +
        "; (" +
        edge.node1.triple +
        ", " +
        edge.node2.triple +
        ")")
  )
}


object UCGraph {
  def apply(ucgNodes: List[BGPNode]): UCGraph =
    new UCGraph(ucgNodes: List[BGPNode])
}