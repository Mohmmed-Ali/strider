package engine.core.sparql

/**
  * Created by xiangnanren on 28/11/2016.
  */
sealed trait TemplateMapping extends java.io.Serializable

/**
  * NodeTemplate and TripleTemplate are two replacement for
  * org.apache.jena.graph.Node and org.apache.jena.graph.Triple.
  * Since these two jena classes are not serializable which means they
  * can not be passes directly into RDD/Dataset closure, so it has
  * to create their mapping to support future query evaluation.
  *
  * @param isVariable : determine the current node is variable/concrete or not.
  * @param mapping    : the serializable mapping of original node.
  */
case class NodeMapping(isVariable: Boolean,
                       mapping: String) extends TemplateMapping

/**
  * @param subjectNode   : the serializable mapping of original subject node.
  * @param predicateNode : the serializable mapping of original predicate node.
  * @param objectNode    : the serializable mapping of original object node.
  */
case class TripleMapping(subjectNode: NodeMapping,
                         predicateNode: NodeMapping,
                         objectNode: NodeMapping) extends TemplateMapping
