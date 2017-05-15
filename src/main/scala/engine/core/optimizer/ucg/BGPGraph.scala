package engine.core.optimizer.ucg

/**
  * Created by xiangnanren on 08/11/2016.
  */
abstract class BGPGraph {

  var visited: Boolean
  val heuristicWeight: Int
  var statisticWeight: Long

  def getInfo: String

  def show(): Unit

}
