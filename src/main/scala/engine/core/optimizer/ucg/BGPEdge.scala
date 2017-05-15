package engine.core.optimizer.ucg

import engine.core.optimizer.ExecutionPlanType.{AdaptiveBushy, StaticBushy}
import engine.core.optimizer.{ExecutionPlanType, InvalidEPException}
import org.apache.jena.graph


/**
  * Created by xiangnanren on 02/11/2016.
  */
class BGPEdge(val node1: BGPNode,
              val node2: BGPNode) extends BGPGraph {
  val joinKey: Seq[String] = initJoinKey
  private val isUnboundedStarJoin: Boolean =
    if (node1.isStarJoin(node2)) {
      if (node1.triple.getObject.isVariable &&
          node2.triple.getObject.isVariable) true
      else false
    } else false

  // An indicator which dedicates the current edge
  // has already been visited by optimizer or not
  override var visited: Boolean = false
  //  override val heuristicWeight: Int =
  //    edgeHeuristicWeight(node1.triple, node2.triple)
  override val heuristicWeight: Int = sumNodesWeight(node1, node2)
  override var statisticWeight: Long = 0L

  def initJoinKey: Seq[String] = {
    for (nodeName <- node1.schema if node2.schema contains nodeName)
      yield nodeName
  }


  /**
    * A method to determine the priority of the join within two given
    * triple patterns. The heuristic rule estimates the triple pattern
    * selectivity is defined by considering the position of join variable,
    * i.e.:
    * P-O < S-P < S-O < O-O < S-S < P-S
    *
    * @param tp1 : first triple pattern
    * @param tp2 : the connected triple pattern of tp1
    * @return : priority(weight) of the join of triple patterns
    */
  protected final def edgeHeuristicWeight(tp1: graph.Triple,
                                          tp2: graph.Triple): Int = tp1 match {
    // P-O, O-P join
    case _tp1 if _tp1.getPredicate.matches(tp2.getObject) &&
      _tp1.getPredicate.isVariable => 1
    case _tp1 if _tp1.getObject.matches(tp2.getPredicate) &&
      _tp1.getObject.isVariable => 1
    // S-P, P-S join
    case _tp1 if _tp1.getSubject.matches(tp2.getPredicate) &&
      _tp1.getSubject.isVariable => 2
    case _tp1 if _tp1.getPredicate.matches(tp2.getSubject) &&
      _tp1.getPredicate.isVariable => 2
    // S-O, O-S join
    case _tp1 if _tp1.getSubject.matches(tp2.getObject) &&
      _tp1.getSubject.isVariable => 3
    case _tp1 if _tp1.getObject.matches(tp2.getSubject) &&
      _tp1.getObject.isVariable => 3
    // O-O join
    case _tp1 if _tp1.getObject.matches(tp2.getObject) &&
      _tp1.getObject.isVariable => 4
    // S-S join
    case _tp1 if _tp1.getSubject.matches(tp2.getSubject) &&
      _tp1.getSubject.isVariable => 5
    // P-P join
    case _tp1 if _tp1.getPredicate.matches(tp2.getPredicate) &&
      _tp1.getPredicate.isVariable => 6
  }


  /**
    * Set the weight of a given edge (i.e. tuple of SparkTriples),
    * the weight equals to the sum of two nodes' heuristic weight.
    */
  protected final def sumNodesWeight(node1: BGPNode,
                                     node2: BGPNode): Int = {
    node1.heuristicWeight + node2.heuristicWeight
  }


  /**
    * This method initialize the weight of current UCG edge.
    * The weight of current UCG edge implies the cardinality
    * the join pattern Card(tp1, tp2). Generally, we distinguish
    * two types of join patterns:
    *
    * 1. star join pattern.
    *    (1). star join pattern of unbounded object.
    *         For a star join group SJ, where tp1, tp2 belongs to SJ.
    *
    *         lambda = min(Card(tpi)) with tpi belongs to SJ
    *         Card(tp1, tp2) = lambda * Card(tp1)/lambda * Card(tp2) / lambda
    *                        = Card(tp1) * Card(tp2) / lambda
    *
    *    (2). star join pattern of bounded object.
    *         Card(tp1, tp2) = min(Card(tp1), Card(tp2))
    *
    * 2. Non star join pattern.
    *    Card(tp1, tp2) = min(Card(tp1), Card(tp2))
    *
    * Here the value returned by the function getMinUnboundedWeight is lambda.
    *
    * @return
    */
  def updateEdgeStaticWeight(): Unit = {
    this.statisticWeight = if(this.node1.starJoinUnbounded.nonEmpty){
      if (this.isUnboundedStarJoin){
        val lambda = node1.getMinUnboundedWeight
        if(lambda.get != 0L)
          node1.statisticWeight * node2.statisticWeight / node1.getMinUnboundedWeight.get
        else 0L
      }
      else node1.statisticWeight min node2.statisticWeight
    } else node1.statisticWeight min node2.statisticWeight
  }


  /**
    * The method compares the weight between two edges and returns
    * the BGPEdge with lighter weight. Two types of weight are supported:
    * heuristic or statistic. Use epType to specify which type of weight
    * should be considered (i.e. HeuByEdge or StatByEdge).
    *
    * @param _edge  : the second BGPEdge to be compared
    * @param epType : type of execution plan, i.e. HeuByEdge or StatByEdge
    * @tparam T : input type of execution plan should be the sub-class of ExecutionPlanType
    * @return : the BGPEdge with lighter weight
    */
  def minWeightEdge[T <: ExecutionPlanType](_edge: BGPEdge,
                                            epType: T): BGPEdge = {
    epType match {

      /**
        * Tricky part: to avoid create left deep binary tree
        * (in case of equivalent edge weight), try to switch
        * the current edge to different edge.
        *
        * I.e., we also aways try to find the connected edges to current
        * node for the query plan generation.
        *
        */
      case StaticBushy =>
        if (this.heuristicWeight .> (_edge.heuristicWeight))
          _edge else this

      case AdaptiveBushy =>
        if (this.statisticWeight .> (_edge.statisticWeight))
          _edge else this

      case _ => throw InvalidEPException("Invalid execution plan type, " +
        "the type should be either HeuByEdge or StatByEdge.")
    }
  }


  /**
    * The method compares the weight between two edges and returns
    * the BGPEdge with heavier weight. Two types of weight are supported:
    * heuristic or statistic. Use epType to specify which type of weight
    * should be considered (i.e. HeuByEdge or StatByEdge).
    *
    * @param _edge  : the second BGPEdge to be compared
    * @param epType : type of execution plan, i.e. HeuByEdge or StatByEdge
    * @tparam T : input type of execution plan should be the sub-class of ExecutionPlanType
    * @return : the BGPEdge with heavier weight
    */
  def maxWeightEdge[T <: ExecutionPlanType](_edge: BGPEdge,
                                            epType: T): BGPEdge = {
    epType match {

      case StaticBushy =>
        if (this.heuristicWeight >= _edge.heuristicWeight)
          this else _edge

      case AdaptiveBushy =>
        if (this.statisticWeight >= _edge.statisticWeight)
          this else _edge

      case _ => throw InvalidEPException("Invalid execution plan type, " +
        "the type should be either StaticBushy or AdapBushy.")
    }
  }


  override def show(): Unit = println(
    "<" +
      node1.triple + " (" + node1.visited + ")" +
      ", " +
      node2.triple + " (" + node2.visited + ")" +
      ">" + " (" + this.visited + ") ")

  override def toString: String = "<" + node1.triple + ", " +
    node2.triple + ">"

  override def getInfo: String = {
    "<" + node1.triple + ", " + node2.triple + ">" +
      s", ( statistic weight: " +
      s"< left = ${node1.statisticWeight}, " +
      s"right = ${node2.statisticWeight} >, " +
      s"edge = ${this.statisticWeight} )"
  }
}

object BGPEdge {
  def apply(node1: BGPNode,
            node2: BGPNode): BGPEdge = new BGPEdge(node1, node2)
}

