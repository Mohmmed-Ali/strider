package engine.core.sparkop.op.litematop

import engine.core.optimizer.ucg.BGPNode
import org.apache.jena.graph

/**
  * Created by xiangnanren on 15/06/2017.
  */
class LiteMatEPHandler(initialBGPNodes: Seq[BGPNode]) {
  val liteMatBGPNodeMapping = initLiteMatBGPNodes(initialBGPNodes)

  private def initLiteMatBGPNodes(initialEP: Seq[BGPNode]):
  Map[graph.Triple, LiteMatBGPNode] = {
    (for (g <- initialEP)
      yield {
        g.triple -> LiteMatBGPNode(g)
      }).toMap
  }
}

object LiteMatEPHandler {
  def apply(initialBGPNodes: Seq[BGPNode]): LiteMatEPHandler =
    new LiteMatEPHandler(initialBGPNodes)
}