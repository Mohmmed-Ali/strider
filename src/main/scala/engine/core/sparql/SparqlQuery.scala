package engine.core.sparql

import engine.core.sparkop.compiler.AlgebraTransformer
import engine.core.sparkop.op.SparkOp
import org.apache.jena.graph
import org.apache.jena.query.Query
import org.apache.jena.sparql.algebra.{Algebra, Op}

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 25/11/2016.
  */

/**
  * Strider currently considers the SPARQL queries consist
  * of four general types: "SELECT", "CONSTRUCT", "ASK", "DESCRIBE"
  *
  * Note that in jena ARQ, the "CONSTRUCT", "ASK", "DESCRIBE" clauses
  * DO NOT belong to the part of query algebra. To evaluate
  * the queries of such types, it has to evaluate the "CONSTRUCT",
  * "ASK", and "DESCRIBE" clauses at the top of algebra tree.
  *
  */
abstract class SparqlQuery(val query: Query, val id: String) extends java.io.Serializable {
  val opRoot: Op = Algebra.compile(query)
  val algebraTransformer: AlgebraTransformer = new AlgebraTransformer()
  val sparkOpRoot: SparkOp = algebraTransformer.transform(opRoot)

  override def toString: String = query.toString
}


/**
  * Select type query, all the parameters belong to the algebra tree.
  * I.e, to evaluate the query, just take the method `executeAlgebra`
  * directly from super class, no additional solution modifier is necessary
  * is needed at the top of the algebra tree.
  *
  * @param query : Input jena Query object
  */
case class SelectQuery(override val query: Query,
                       override val id: String) extends SparqlQuery(query, id) {
  def this(query: Query) = this(query, "")
}

object SelectQuery {
  def apply(query: Query) = new SelectQuery(query)
}


/**
  * The evaluation of construct type query consists of two steps:
  *
  * step 1) evaluate the part of algebra.
  * step 2) add additional solution modifier at the top of already-evaluated
  * algebra to obtain the final query result.
  *
  * @param query : Input jena Query object
  */
case class ConstructQuery(override val query: Query,
                          override val id: String) extends SparqlQuery(query, id) {
  def this(query: Query) = this(query, "")

  val constructTemplate: List[graph.Triple] =
    query.getConstructTemplate.getTriples.toList

  val templateMapping = constructMapping(constructTemplate)

  /**
    * Get the mapping of construct template.
    * Generally, for each triple in the original template, if the node
    * of the triple is variable (resp. concrete), the method returns the
    * value of corresponding field from already-computed algebra-part DataFrame,
    * or the method returns the value as the template required
    * (e.g. URI, Literal, or Blank Node label)
    *
    * @param triples : original jena construct template
    * @return : the mapping of original template
    */
  private def constructMapping(triples: List[graph.Triple]): List[TripleMapping] = {
    triples.map(triple => {

      val subjectNode = triple.getSubject match {
        case node if node.isVariable =>
          NodeMapping(true: Boolean, node.getName)

        case node if node.isURI =>
          NodeMapping(false: Boolean, "<" + node.getURI + ">")

        case node if node.isLiteral =>
          NodeMapping(false: Boolean, node.getLiteral.toString)

        case node if node.isBlank =>
          NodeMapping(false: Boolean, node.getBlankNodeLabel)
      }

      val predicateNode = triple.getPredicate match {
        case node if node.isVariable =>
          NodeMapping(true: Boolean, node.getName)

        case node if node.isURI =>
          NodeMapping(false: Boolean, "<" + node.getURI + ">")

        case node if node.isLiteral =>
          NodeMapping(false: Boolean, node.getLiteral.toString)

        case node if node.isBlank =>
          NodeMapping(false: Boolean, node.getBlankNodeLabel)
      }

      val objectNode = triple.getObject match {
        case node if node.isVariable =>
          NodeMapping(true: Boolean, node.getName)

        case node if node.isURI =>
          NodeMapping(false: Boolean, "<" + node.getURI + ">")

        case node if node.isLiteral =>
          NodeMapping(false: Boolean, node.getLiteral.toString)

        case node if node.isBlank =>
          NodeMapping(false: Boolean, node.getBlankNodeLabel)
      }
      TripleMapping(subjectNode, predicateNode, objectNode)
    })
  }
}

object ConstructQuery {
  def apply(query: Query) = new ConstructQuery(query)
}


case class AskQuery(override val query: Query,
                    override val id: String) extends SparqlQuery(query, id) {
  def this(query: Query) = this(query, "")
}

object AskQuery {
  def apply(query: Query) = new AskQuery(query)
}


case class DescribeQuery(override val query: Query,
                         override val id: String) extends SparqlQuery(query, id) {
  def this(query: Query) = this(query, "")
}

object DescribeQuery {
  def apply(query: Query) = new DescribeQuery(query)
}


