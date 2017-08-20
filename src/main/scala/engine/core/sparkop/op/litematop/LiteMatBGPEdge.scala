package engine.core.sparkop.op.litematop

import engine.core.label.SparkOpLabels
import engine.core.optimizer.ucg.BGPEdge
import org.apache.spark.sql._

/**
  * Created by xiangnanren on 18/06/2017.
  */
case class LiteMatBGPEdge(bgpEdge: BGPEdge) extends LiteMatBGPGraph {

  def compute(leftDF: DataFrame,
              rightDF: DataFrame): DataFrame =
    leftDF.join(rightDF,
      bgpEdge.joinKey,
      SparkOpLabels.BGP_INNER_JOIN)

}