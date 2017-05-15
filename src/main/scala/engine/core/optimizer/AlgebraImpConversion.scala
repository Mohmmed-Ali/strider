package engine.core.optimizer

import engine.core.optimizer.ucg.BGPNode
import org.apache.jena.graph

/**
  * Created by xiangnanren on 04/11/2016.
  */
private[optimizer] object AlgebraImpConversion {

  /**
    * Implicit conversion between SparkTriple and jena.graph.Triple
    */
  implicit def bgpNode2Triple(node: BGPNode): graph.Triple = node.triple

  implicit def triple2BgpNode(tp: graph.Triple): BGPNode = BGPNode(tp)

  /**
    * Implicit conversion between List[SparkTriple] and List[jena.graph.Triple]
    */
  implicit def bgpNodeList2TpList(nodeList: List[BGPNode]):
  List[graph.Triple] = {
    nodeList.map(x => x.triple)
  }

  implicit def tpList2NodeList(tpList: List[graph.Triple]):
  List[BGPNode] = {
    tpList.map(x => BGPNode(x))
  }
}
