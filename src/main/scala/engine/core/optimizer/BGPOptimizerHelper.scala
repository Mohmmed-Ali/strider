package engine.core.optimizer

import engine.core.optimizer.ExecutionPlanType.{AdaptiveBushy, AdaptiveGreedy, StaticBushy, StaticGreedy}
import engine.core.optimizer.ucg.{BGPEdge, BGPGraph, BGPNode}
import org.apache.jena.graph

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Created by xiangnanren on 01/11/2016.
  */
trait BGPOptimizerHelper {

  protected def isIntersected(edge1: BGPEdge,
                              edge2: BGPEdge): Boolean = edge1 match {
    case _edge1 if matches(_edge1, edge2) => false

    case _edge1 if matches(_edge1.node1, edge2.node1) => true

    case _edge1 if matches(_edge1.node1, edge2.node2) => true

    case _edge1 if matches(_edge1.node2, edge2.node1) => true

    case _edge1 if matches(_edge1.node2, edge2.node2) => true

    case _ => false
  }

  protected def edgeContains(node: BGPNode,
                             edge: BGPEdge): Boolean = {
    if (matches(edge.node1, node) && !matches(edge.node2, node))
      true
    else if (!matches(edge.node1, node) && matches(edge.node2, node))
      true
    else false
  }

  /**
    * Return the different node of two connected edges, e.g.:
    * edge1: (node1, node2), edge 1 is the current edge.
    * edge2: (node1, node3), edge 2 is the external edge connects to edge1
    * return node3
    *
    * @param currentEdge: Current BGPEdge
    * @param externalEdge: Another BGPEdge to be compared
    * @return :
    */
  protected final def findDiffNode(currentEdge: BGPEdge,
                                   externalEdge: BGPEdge): BGPNode =
    currentEdge match {
      case _currentEdge if matches(_currentEdge.node1, externalEdge.node1) ||
        matches(_currentEdge.node2, externalEdge.node1)
      => externalEdge.node2

      case _currentEdge if matches(_currentEdge.node1, externalEdge.node2) ||
        matches(_currentEdge.node2, externalEdge.node2)
      => externalEdge.node1

    }

  protected final def seqContains[S <: BGPGraph](graph: S,
                                                 graphSeq: Seq[S]): Boolean = {
    (graph, graphSeq) match {
      case (
        _graph: BGPNode,
        _graphSeq: Seq[BGPNode@unchecked]
        ) => _graphSeq.exists(x => matches(x, _graph))

      case (
        _graph: BGPEdge,
        _graphSeq: Seq[BGPEdge@unchecked]
        ) => _graphSeq.exists(x => matches(x, _graph))

      case _ => false
    }
  }

  protected final def matches[S <: BGPGraph](g1: S,
                                             g2: S): Boolean = {
    (g1, g2) match {
      case (g1: BGPNode, g2: BGPNode)
        if g1.triple.matches(g2.triple) => true

      case (g1: BGPEdge, g2: BGPEdge)
        if g1.node1.triple.matches(g2.node1.triple) &&
          g1.node2.triple.matches(g2.node2.triple) => true

      case (g1: BGPEdge, g2: BGPEdge)
        if g1.node1.triple.matches(g2.node2.triple) &&
          g1.node2.triple.matches(g2.node1.triple) => true

      case _ => false
    }
  }

  protected final def setContains[S <: BGPGraph](graph: S,
                                                 graphSet: mutable.Set[S]): Boolean = {
    (graph, graphSet) match {
      case (
        _graph: BGPNode,
        _graphSet: mutable.Set[BGPNode@unchecked]
        ) => _graphSet.exists(x => matches(x, _graph))

      case (
        _graph: BGPEdge,
        _graphSet: mutable.Set[BGPEdge@unchecked]
        ) => _graphSet.exists(x => matches(x, _graph))

      case _ => false
    }
  }

  /**
    * A method determines whether current UCG could be a candidate
    * for the BGP execution plan or not. I.e. a given BGPGraph is whether
    * connected to a collection BGPGraph or not.
    *
    */
  protected final def isCandidate[S <: BGPGraph, T <: BGPGraph](graph: S,
                                                                graphSet: mutable.Set[T]): Boolean = {
    val c = graphSet.count(g => graphConnection(graph, g))
    c match {
      case size if size > 0 => true
      case size if size == 0 => false
    }
  }

  /**
    * A method determines whether two sub UCGs (node or edge) are connected or not.
    * I.e.: determines the conncetion between (node, node), (node, edge),
    * (edge, node), or (edge, edge)
    *
    * @param g1 : first UCG
    * @param g2 : second UCG
    * @tparam S : type parameter, specifies the upper bound of g1
    * @tparam T : type parameter, specifies the upper bound of g2
    */
  protected final def graphConnection[S <: BGPGraph, T <: BGPGraph](g1: S,
                                                                    g2: T): Boolean = {
    (g1, g2) match {
      case (_g1: BGPNode, _g2: BGPNode) =>
        isConnected(_g1.triple, _g2.triple)

      case (_g1: BGPNode, _g2: BGPEdge)
        if isConnected(_g1.triple, _g2.node1.triple) ||
          isConnected(_g1.triple, _g2.node2.triple) => true

      case (_g1: BGPEdge, _g2: BGPNode)
        if isConnected(_g1.node1.triple, _g2.triple) ||
          isConnected(_g1.node2.triple, _g2.triple) => true

      case (_g1: BGPEdge, _g2: BGPEdge)
        if isConnected(_g1.node1.triple, _g2.node1.triple) ||
          isConnected(_g1.node1.triple, _g2.node2.triple) ||
          isConnected(_g1.node2.triple, _g2.node1.triple) ||
          isConnected(_g1.node2.triple, _g2.node2.triple) => true

      case _ => false

    }
  }

  /**
    * Basic util method, determine whether two triple patterns are connected or not.
    * Only consider the unbound terms of triple patterns could be join keys.
    * Also used to determine the connection for:
    *
    * 1) SparkTriple - _SparkTriple,
    * 2) SparkTripleTuple2 - _SparkTripleTuple2,
    * 3) SparkTripleTuple2 - SparkTriple
    *
    * @param tp1 : first triple pattern
    * @param tp2 : second triple pattern
    * @return : Boolean value, true for connected,
    *         false for non-connected
    */
  protected final def isConnected(tp1: graph.Triple,
                                  tp2: graph.Triple): Boolean = {
    tp1 match {
      case _tp1 if _tp1.getSubject.matches(tp2.getSubject)
        && _tp1.getSubject.isVariable => true

      case _tp1 if _tp1.getSubject.matches(tp2.getPredicate)
        && _tp1.getSubject.isVariable => true

      case _tp1 if _tp1.getSubject.matches(tp2.getObject)
        && _tp1.getSubject.isVariable => true

      case _tp1 if _tp1.getPredicate.matches(tp2.getSubject)
        && _tp1.getPredicate.isVariable => true

      case _tp1 if _tp1.getPredicate.matches(tp2.getPredicate)
        && _tp1.getPredicate.isVariable => true

      case _tp1 if _tp1.getPredicate.matches(tp2.getObject)
        && _tp1.getPredicate.isVariable => true

      case _tp1 if _tp1.getObject.matches(tp2.getSubject)
        && _tp1.getObject.isVariable => true

      case _tp1 if _tp1.getObject.matches(tp2.getPredicate)
        && _tp1.getObject.isVariable => true

      case _tp1 if _tp1.getObject.matches(tp2.getObject)
        && _tp1.getObject.isVariable => true

      case _ => false
    }
  }

  /**
    * Return the node with minimum node weight in a given UCG.
    * Two types of weight are supported: heuristic (i.e. static execution plan)
    * weight or statistic (i.e. adaptive execution plan) weight.
    * The method finds the edge of minimum weight by tail recursion.
    *
    */
  protected final def minUCGNode[T <: ExecutionPlanType](inits: Seq[BGPNode],
                                                         epType: T): BGPNode = {
    val accum = inits.head

    @tailrec
    def minStatAccum(inits: Seq[BGPNode],
                     accum: BGPNode): BGPNode = inits match {
      case Nil => accum

      case x :: tail => val minNode = x.minNode(accum, StaticGreedy)
        minStatAccum(tail, minNode)
    }

    @tailrec
    def minAdapAccum(inits: Seq[BGPNode],
                     accum: BGPNode): BGPNode = inits match {
      case Nil => accum

      case x :: tail => val minNode = x.minNode(accum, AdaptiveGreedy)
        minAdapAccum(tail, minNode)
    }

    epType match {
      case StaticGreedy => minStatAccum(inits, accum)

      case AdaptiveGreedy => minAdapAccum(inits, accum)

      case _ => throw InvalidEPException("Invalid execution plan type, " +
        "the type should be either Greedy or Bushy.")
    }
  }


  /**
    * Return the edge with minimum edge weight in a given UCG.
    * Two types of weight are supported: heuristic weight or statistic weight.
    * The method finds the edge of minimum weight by tail recursion.
    *
    * @param inits  : input UCG.
    * @param epType : type of execution plan determines the type of
    *               edge weight to be considered (i.e. HeuByEdge or StatByEdge).
    * @tparam T : input type of execution plan should be the sub-class of ExecutionPlanType
    * @return : the edge with minimum edge weight in inits.
    */
  protected final def minUCGEdge[T <: ExecutionPlanType](inits: Seq[BGPEdge],
                                                         epType: T):
  BGPEdge = {

    @tailrec
    def minStatAccum(inits: Seq[BGPEdge],
                     accum: BGPEdge):
    BGPEdge = inits match {
      case Nil => accum

      case x :: tail => minStatAccum(tail, x.minWeightEdge(accum, StaticBushy))
    }

    @tailrec
    def minAdapAccum(inits: Seq[BGPEdge],
                     accum: BGPEdge):
    BGPEdge = inits match {
      case Nil => accum

      case x :: tail => minAdapAccum(tail, x.minWeightEdge(accum, AdaptiveBushy))
    }

    epType match {
      case StaticBushy => minStatAccum(inits, inits.head)

      case AdaptiveBushy => minAdapAccum(inits, inits.head)

      case _ => throw InvalidEPException("Invalid execution plan type, " +
        "the type should be either static bushy or adaptive bushy.")
    }
  }
}
