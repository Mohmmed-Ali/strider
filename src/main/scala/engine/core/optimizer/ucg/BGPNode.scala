package engine.core.optimizer.ucg

import engine.core.optimizer.ExecutionPlanType.{AdaptiveGreedy, StaticGreedy}
import engine.core.optimizer.{BGPOptimizerHelper, ExecutionPlanType, InvalidEPException}
import org.apache.jena.graph
import org.apache.jena.graph.Node

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 01/11/2016.
  */
class BGPNode(val triple: graph.Triple,
              var statisticWeight: Long)
  extends BGPGraph with BGPOptimizerHelper {

  override var visited: Boolean = false
  override val heuristicWeight: Int = nodeHeuristicWeight(triple)
  val schema: Seq[String] = getSchema
  val starJoinUnbounded = new ArrayBuffer[BGPNode]()

  /**
    * Auxiliary constructor, the statistic weight will
    * be replaced by zero when it is absent
    *
    * @param triple : class is initialized by a triple pattern
    */
  def this(triple: graph.Triple) {
    this(triple, 0L)
  }

  /**
    * Return the BGPNode with the minimum weight.
    * The weight can be heuristic weight or statistic weight.
    *
    * @param _bgpNode : external BGPNode to be compared.
    * @param epType   : Type of execution plan, it concerns the weight type
    *                 to be compared (i.e. heuristic weight or statistic weight)
    * @tparam T : type parameter, the type of execution plan should be the
    *           sub-class of ExecutionPlanType.
    * @return : the BGPNode with lighter node weight.
    */
  def minNode[T <: ExecutionPlanType](_bgpNode: BGPNode,
                                      epType: T): BGPNode = epType match {
    case StaticGreedy =>
      if (this.heuristicWeight <= _bgpNode.heuristicWeight)
        this
      else _bgpNode

    case AdaptiveGreedy =>
      if (this.statisticWeight <= _bgpNode.statisticWeight)
        this
      else _bgpNode

    case _ => throw InvalidEPException("Invalid execution plan type, " +
      "the type should be either HeuByEdge or StatByEdge.")
  }

  /**
    * Setter of obtaining all star join node of unbounded
    * object for current node.
    *
    * @param ucgNodes : The list of all BGPNode in UCG.
    */
  def setStarJoinNodes(ucgNodes: List[BGPNode]): BGPNode = {
    ucgNodes.foreach(node => {
      if (isStarJoin(node)) {
        if (this.triple.getObject.isVariable &&
          node.triple.getObject.isVariable) starJoinUnbounded.append(node)
      }
    })
    this
  }

  /**
    * Distinguish whether a given triple pattern (BGPNode)
    * is connected with current triple pattern in a star format
    *
    * @param _node : an input node
    */
  def isStarJoin(_node: BGPNode): Boolean = {
    if (!graphConnection(this, _node)) false
    else {
      if (this.triple.getSubject.matches(_node.triple.getSubject)) true
      else false
    }
  }

  /**
    * Get the minimum weight of a unbounded star join.
    * E.g.,
    * ?s   p1   ?o1;  weight = 1000
    * p2   ?o2;  weight = 2000
    * p3   ?o3.  weight = 3000
    *
    * The minimum weight here = 1000
    *
    * @return If exists, return the minimum weight of a
    *         unbounded star join. Otherwise, return none
    *
    */
  def getMinUnboundedWeight: Option[Long] = {
    if (starJoinUnbounded.nonEmpty) {
      val tempNode = starJoinUnbounded.
        reduceLeft((node1, node2) => {
          if (node1.statisticWeight >= node2.statisticWeight) node2
          else node1
        })
      Some(tempNode.statisticWeight)
    }
    else {
      None
    }
  }

  override def show(): Unit = println(triple + " (" + this.visited + ") ")

  override def toString: String = triple.toString()

  override def getInfo: String = triple.toString() +
    s" ( static weight: < $statisticWeight > )"

  /**
    * @return : The schema of a given triple pattern,
    *         it presents the list of variables' names
    */
  private def getSchema: Seq[String] = {
    val schema: Seq[Node] = Seq(triple.getSubject, triple.getPredicate, triple.getObject)

    for (n <- schema if n.isVariable) yield n.getName
  }

  /**
    * A method to determine the priority of triple pattern.
    * The heuristic rule estimates the triple pattern selectivity
    * is defined by considering two dimensions, e.g. the number of bound
    * or unbound node and the position of the node in a triple pattern.
    * I.e.:
    *
    * (s, p, o) < (s, ?, o) < (?, p, o) < (s, p, ?) <
    * (?, ?, o) < (s, ?, ?) < (?, p, ?) < (?, ?, ?)
    *
    * Each form of triple pattern is assigned with an integer value (heuristic weight)
    * from 1 to 9 over the descending order of triple pattern selectivity
    *
    * @param triple : input triple pattern
    * @return : priority(weight) of triple pattern
    */
  private final def nodeHeuristicWeight(triple: graph.Triple): Int = (
    triple.getSubject.isVariable,
    triple.getPredicate.isVariable,
    triple.getObject.isVariable
    ) match {
    // (s, p, o)
    case (false, false, false) => 1
    // (s, ?, o)
    case (false, true, false) => 2
    // (?, p, o)
    case (true, false, false) => 3
    // (s, p, ?)
    case (false, false, true) => 4
    // (?, ?, o)
    case (true, true, false) => 5
    // (s, ?, ?)
    case (false, true, true) => 6
    // (?, p, ?)
    case (true, false, true) => 7
    // (?, ?, ?)
    case (true, true, true) => 8
  }
}


object BGPNode {

  def apply(triple: graph.Triple,
            stat: Long): BGPNode = new BGPNode(triple, stat)

  def apply(triple: graph.Triple): BGPNode = new BGPNode(triple)

}