package engine.core.optimizer.ucg

/**
  * Created by xiangnanren on 08/11/2016.
  */
abstract class BGPGraph {

  val heuristicWeight: Int
  var visited: Boolean
  var statisticWeight: Long

  def getInfo: String

  def show(): Unit

}
