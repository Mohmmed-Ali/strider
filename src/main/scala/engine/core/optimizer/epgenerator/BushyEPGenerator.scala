package engine.core.optimizer.epgenerator

import engine.core.optimizer.ExecutionPlanType.{AdaptiveBushy, StaticBushy}
import engine.core.optimizer._
import engine.core.optimizer.ucg.{BGPEdge, BGPGraph, BGPNode, UCGraph}

import scala.collection.immutable.List
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xiangnanren on 06/12/2016.
  */
class BushyEPGenerator[T <: ExecutionPlanType](epType: T)
  extends EPGenerator {

  /**
    * Generate execution plan for each BGP operator.
    * This method will be meaningful if and only if the number
    * of triple patterns in the BGP is >= 2.
    *
    * The execution plan is basically a binary tree, every leaf node
    * presents an unary operator, i.e. the projection on the triple pattern,
    * the intermediate nodes present the binary operator, i.e. join between
    * two triple patterns (or group of triple patterns).
    *
    * The construction of execution plan is essentially the construction of a
    * binary tree from bottom up, it consists of two steps:
    *
    * 1) Generate a undirected connect graph which present the possible
    * optimized plan for query execution;
    * 2) Convert the obtained UCG in 1) into a (bushy) binary tree, the binary
    * tree is essentially a stack, it is built by post-order traversal.
    * E.g., tp1 join tp2 => tp1 tp2 join(tp1, tp2),more precisely:
    *
    * |  join(tp1, tp2)  |
    * |  tp1 (resp. tp2) |  the order of tp1 and tp2 does not matter,
    * |  tp2 (resp. tp1) |  since it does not influence for the computation.
    *
    * @param g : the initial undirected connect graph of a gieven BGP operator
    * @return : optimized execution plan
    */
  override def generate(g: UCGraph): List[BGPGraph] = {
    val ucgEdges: List[BGPEdge] = g.ucgEdges
    ucgEdges.foreach(edge => edge.visited = false)
    val ucgNodes: List[BGPNode] = g.ucgNodes
    ucgNodes.foreach(node => node.visited = false)
    val candidates: ArrayBuffer[BGPEdge] = createCandidates(g)

    createTree(candidates).toList
  }


  /**
    * The method generates the edge candidates for query execution plan.
    * For a given UCG graph (with edge weight), edge candidates is generated
    * by traversing all the nodes in UCG and link them together. The obtained
    * edge candidates should guarantee that the sum of edge weights are the minimum
    * (or one of the minimum, since some edges possess the same edge weights)
    *
    * @param g : the query UCG
    * @return : edges candidates with minimum edge weights.
    */
  private[this] def createCandidates(g: UCGraph): ArrayBuffer[BGPEdge] = {
    val candidates = new ArrayBuffer[BGPEdge]

    val initMinEdge = minUCGEdge(g.ucgEdges, epType)

    candidates.append(initMinEdge)
    markVisited(initMinEdge, g)

    while (g.ucgNodes.exists(node => !node.visited)) {
      val tempUCG = initTempUCG(g.ucgEdges)
      val edgeCandidates = createIntersectEdges(candidates, tempUCG)
      val currentMinEdge = minUCGEdge(edgeCandidates, epType)

      candidates.append(currentMinEdge)
      markVisited(currentMinEdge, g)
    }
    candidates
  }

  /**
    * Initialize the temporary UCG to create edge candidates,
    * all the edges of temporary UCG are not visited.
    *
    * @param ucgEdges : edges in UCG
    * @return : temporary UCG
    */
  private[this] def initTempUCG(ucgEdges: List[BGPEdge]): List[BGPEdge] = {
    for (edge <- ucgEdges
         if !edge.visited) yield edge
  }

  /**
    * Get all non-visited edges which are intersected to the given edges
    */
  private[this] def createIntersectEdges(candidates: Seq[BGPEdge],
                                         tempUCG: Seq[BGPEdge]): List[BGPEdge] = {
    for (
      i <- candidates;
      j <- tempUCG
      if isIntersected(i, j) && !j.visited
    ) yield j

  }.toList

  /**
    * Mark the UCG nodes and edges with respect to the given edge
    *
    * @param edge : the edge (include the two nodes of the edge) to
    *             be remarked as visited
    * @param ucg  : The query UCG
    */
  private[this] def markVisited(edge: BGPEdge,
                                ucg: UCGraph): Unit = {
    edge.node1.visited = true
    edge.node2.visited = true

    ucg.ucgEdges.foreach(_edge => {
      if (_edge.node1.visited && _edge.node2.visited)
        _edge.visited = true
    })
  }

  /**
    * Recursively create (top down) the abstract syntax tree of execution plan.
    *
    * @param candidates : linked edges with the lightest total edge weight
    * @return : execution plan of suffix expression (i.e. post-order traversal of binary tree)
    */
  private[this] def createTree(candidates: ArrayBuffer[BGPEdge]): mutable.Stack[BGPGraph] = {

    val stack = new mutable.Stack[BGPGraph]
    val root = candidates.reduce((edge1, edge2) => edge1.maxWeightEdge(edge2, epType))

    candidates -= root
    insertTreeNode(root, stack)

    /**
      * Find the child node of the input BGPNode.
      * If the child node is a linked BGPEdge, then return
      * the linked edge with the maximum edge weight.
      *
      * If the child node is a BGPNode, i.e. a leaf node of
      * query execution plan, then return this node.
      *
      * @param node : the input tree node of execution plan
      * @return : child of the input tree node
      */
    def findChildNode(node: BGPNode): BGPGraph = {

      val tempCandidates = candidates.
        filter(edge => edgeContains(node, edge))

      tempCandidates match {
        case _tempCandidates if _tempCandidates.isEmpty => node

        case _tempCandidates if _tempCandidates.nonEmpty =>
          val child = _tempCandidates.
            reduce((edge1, edge2) => edge1.maxWeightEdge(edge2, epType))
          candidates -= child
          child
      }
    }


    /**
      * The method constructs bushy tree execution plan.
      * Insert the tree nodes with the respect to the post-order traversal.
      * To reduce the intermediate result, BGPEdges with heavier weights
      * locate on the higher level, BGPEdges with lighter weights are
      * computed on the lower level.
      *
      * @param treeNode : input tree node. A tree node is practically either a
      *                 BGPNode or a BGPEdge.
      * @param stack    : presents the traversal order (post-order) of execution plan
      * @return : the execution plan in format of stack, the root of execution plan
      *         locates on the bottom of the stack.
      */
    def insertTreeNode(treeNode: BGPGraph,
                       stack: mutable.Stack[BGPGraph]): mutable.Stack[BGPGraph] = {
      treeNode match {
        case _treeNode: BGPEdge => stack.push(_treeNode)

          val childNode1 = findChildNode(_treeNode.node1)
          val childNode2 = findChildNode(_treeNode.node2)

          (childNode1, childNode2) match {

            case (_childNode1: BGPNode, _childNode2) =>
              insertTreeNode(childNode1, stack)
              insertTreeNode(childNode2, stack)

            case (_childNode1, _childNode2: BGPNode) =>
              insertTreeNode(childNode2, stack)
              insertTreeNode(childNode1, stack)

            case (_childNode1, _childNode2) =>
              insertTreeNode(_childNode1, stack)
              insertTreeNode(_childNode2, stack)

          }
        case _treeNode: BGPNode => stack.push(_treeNode)
      }
    }
    stack
  }
}


object BushyEPGenerator {
  def apply[T <: ExecutionPlanType](epType: T) = epType match {

    case StaticBushy => new BushyEPGenerator(StaticBushy)

    case AdaptiveBushy => new BushyEPGenerator(AdaptiveBushy)

    case _ => throw InvalidEPException("Invalid execution plan type, " +
      "the type should be either HeuByEdge or StatByEdge, " +
      "execution plan generator initialize failed")
  }

}







