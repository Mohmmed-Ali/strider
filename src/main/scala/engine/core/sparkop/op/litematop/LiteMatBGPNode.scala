package engine.core.sparkop.op.litematop

import engine.core.label.LabelBase
import engine.core.optimizer.ucg.BGPNode
import engine.core.sparql.reasoning.{LiteMatCtx, LiteMatRDCT}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ListBuffer

/**
  * Created by xiangnanren on 18/06/2017.
  */
case class LiteMatBGPNode(bgpNode: BGPNode) extends LiteMatBGPGraph {
  // The mapping of a node in a given triple pattern
  // is represented as (rewritable, lowerBound, upperBound)
  final val subjectMapping: (String, Int, Int) =
    setSubjectMapping(bgpNode, LiteMatCtx.RDCT.conceptMapping)
  final val predicateMapping: (String, Int, Int) =
    setPredicateMapping(bgpNode, LiteMatCtx.RDCT.propertyMapping)
  final val objectMapping: (String, Int, Int) =
    setObjectMapping(bgpNode, LiteMatCtx.RDCT.conceptMapping)
  val liteMatFilters = generateFilters(
    List(subjectMapping, predicateMapping, objectMapping))
  val outpuSchema = getOutputSchema(bgpNode)

  def getOutputSchema(bgpNode: BGPNode): Seq[String] = {
    val schema = ListBuffer[String]()

    if (bgpNode.triple.getSubject.isVariable)
      schema.append(bgpNode.triple.getSubject.getName)
    if (bgpNode.triple.getPredicate.isVariable)
      schema.append(bgpNode.triple.getPredicate.getName)
    if (bgpNode.triple.getObject.isVariable)
      schema.append(bgpNode.triple.getObject.getName)

    schema
  }

  def rewritable(dct: LiteMatRDCT): Boolean = {
    if (isSubjectRewritable(bgpNode, dct.conceptMapping)) true
    else if (isPredicateRewritable(bgpNode, dct.propertyMapping)) true
    else if (isObjectRewritable(bgpNode, dct.conceptMapping)) true
    else false
  }

  def quoteString(s: String): String = "<" + s + ">"


  /**
    * Check whether subject is rewritable or not.
    * The dictionary (mapping) of individual is not must be provided.
    */
  private def isSubjectRewritable(bgpNode: BGPNode,
                                  boundConceptDct: Map[String, (String, String)],
                                  boundIndividualDct: Map[String, String] =
                                  Map.empty[String, String]): Boolean = {
    !bgpNode.triple.getSubject.isVariable && (
      boundConceptDct.get(quoteString(bgpNode.triple.getSubject.toString)).nonEmpty ||
        boundIndividualDct.get(quoteString(bgpNode.triple.getSubject.toString)).nonEmpty)
  }

  /**
    * Check whether predicate is rewritable or not
    */
  private def isPredicateRewritable(bgpNode: BGPNode,
                                    boundDct: Map[String, (Int, Int)]): Boolean = {
    val bounds = boundDct.get(quoteString(bgpNode.triple.getPredicate.toString))

    !bgpNode.triple.getPredicate.isVariable &&
      bounds.nonEmpty &&
      (bounds.get._1 != 0) // rdf:type is encoded as 0
  }

  /**
    * Check whether object is rewritable or not.
    * The dictionary (mapping) of individual is not must be provided.
    */
  private def isObjectRewritable(bgpNode: BGPNode,
                                 boundConceptDct: Map[String, (String, String)],
                                 boundIndividualDct: Map[String, String] =
                                 Map.empty[String, String]): Boolean = {
    !bgpNode.triple.getObject.isVariable && (
      boundConceptDct.get(quoteString(bgpNode.triple.getObject.toString)).nonEmpty ||
        boundIndividualDct.get(quoteString(bgpNode.triple.getObject.toString)).nonEmpty)
  }


  /**
    * The method which applies LiteMat filter on concept.
    * For the concept at object position, after encoding,
    * it is necessary to add a prefix to distinguish the encoded
    * value and the primitive value. So, here we just remove
    * the first letter of an encoded value (the defined prefix for LiteMat
    * is just a letter 'E'), then we convert the rest part of the string
    * into integer for further filter operation.
    *
    */
  def conceptsLMUdf(lowerBound: Int,
                    upperBound: Int) = udf(
    (arg: String) => arg.startsWith(LiteMatCtx.conceptPrefix) match {
      case true => val parsedArg = arg.tail.toInt
        parsedArg >= lowerBound && parsedArg < upperBound
      case false => false
    })

  /**
    * The method which applies LiteMat filter on property.
    * Note that the property does not need a prefix to distinguish
    * the encoded value and the primitive type value. So the input
    * parameter "arg" in udf does not need to be removed the encoding prefix.
    *
    */
  def propertiesLMUdf(lowerBound: Int,
                      upperBound: Int) = udf((arg: Int) => {
    arg >= lowerBound && arg < upperBound
  })


  def individualsLMUdf(mapping: String) = udf((arg: String) => arg == mapping)

  def basicUdf(mapping: String) = udf((arg: String) => arg == mapping)

  /**
    * If the subject is rewritable, rewrite the subject to its corresponding mapping.
    *
    * @param boundConceptDct : The dictionary of concepts.
    * @param boundIndividualDct : The dictionary of individual. The individual dictionary
    *                             is initialized as empty, since it is not necessarily required.
    */

  private def setSubjectMapping(bgpNode: BGPNode,
                                boundConceptDct: Map[String, (String, String)],
                                boundIndividualDct: Map[String, String]
                                = Map.empty[String, String]):
  (String, Int, Int) = {
    if (!isSubjectRewritable(bgpNode, boundConceptDct, boundIndividualDct))
      (LiteMatBGPNode.NOT_REWRITABLE, -1, -1)
    else {
      val isCptEmpty = boundConceptDct.
        get(quoteString(bgpNode.triple.getSubject.toString))
      val isIndEmpty = boundIndividualDct.
        get(quoteString(bgpNode.triple.getSubject.toString))

      if (isCptEmpty.nonEmpty)
        (LiteMatBGPNode.CONCEPTS_TYPE,
          isCptEmpty.get._1.replaceFirst(LiteMatCtx.conceptPrefix, "").toInt,
          isCptEmpty.get._2.replaceFirst(LiteMatCtx.conceptPrefix, "").toInt)
      else
        (LiteMatBGPNode.INDIVIDUALS_TYPE,
          isIndEmpty.get.replaceFirst(LiteMatCtx.individualPrefix, "").toInt,
          isIndEmpty.get.replaceFirst(LiteMatCtx.individualPrefix, "").toInt)
    }
  }


  /**
    * If the predicate is rewritable, rewrite the predicate to its corresponding mapping.
    *
    * @param boundPropertyDct : The dictionary of properties.
    */
  private def setPredicateMapping(bgpNode: BGPNode,
                                  boundPropertyDct: Map[String, (Int, Int)]):
  (String, Int, Int) = {
    if (!isPredicateRewritable(bgpNode, boundPropertyDct))
      (LiteMatBGPNode.NOT_REWRITABLE, -1, -1)
    else (LiteMatBGPNode.PROPERTIES_TYPE,
      boundPropertyDct.
        get(quoteString(bgpNode.triple.getPredicate.toString)).get._1,
      boundPropertyDct.
        get(quoteString(bgpNode.triple.getPredicate.toString)).get._2)
  }


  /**
    * If the object is rewritable, rewrite the object to its corresponding mapping.
    *
    * @param boundConceptDct : The dictionary of concepts.
    * @param boundIndividualDct : The dictionary of individuals. The individual dictionary
    *                             is initialized as empty, since it is not necessarily required.
    */
  private def setObjectMapping(bgpNode: BGPNode,
                               boundConceptDct: Map[String, (String, String)],
                               boundIndividualDct: Map[String, String]
                               = Map.empty[String, String]):
  (String, Int, Int) = {
    if (!isObjectRewritable(bgpNode, boundConceptDct))
      (LiteMatBGPNode.NOT_REWRITABLE, -1, -1)
    else {
      val isCptEmpty = boundConceptDct.get(quoteString(bgpNode.triple.getObject.toString))
      val isIndEmpty = boundIndividualDct.get(quoteString(bgpNode.triple.getObject.toString))

      if (isCptEmpty.nonEmpty)
        (LiteMatBGPNode.CONCEPTS_TYPE,
          isCptEmpty.get._1.replaceFirst(LiteMatCtx.conceptPrefix, "").toInt,
          isCptEmpty.get._2.replaceFirst(LiteMatCtx.conceptPrefix, "").toInt)
      else (LiteMatBGPNode.INDIVIDUALS_TYPE,
        isIndEmpty.get.replaceFirst(LiteMatCtx.individualPrefix, "").toInt,
        isIndEmpty.get.replaceFirst(LiteMatCtx.individualPrefix, "").toInt)
    }
  }

  /**
    * This method generates a sequence of filter(udf) operators
    * for current BGP node.
    *
    * @return A tuple sequence in form of (column name, udf).
    *         The first element of each tuple determines which
    *         column to apply a litemat filter.
    */
  private def generateFilters(epMapping: List[(String, Int, Int)]):
  Seq[(String, UserDefinedFunction)] = {
    val filters = ListBuffer[(String, UserDefinedFunction)]()

    /**
      * Construct the equivalent filter function on Subject
      */
    if (epMapping.head._1 != LiteMatBGPNode.NOT_REWRITABLE) {
      if (epMapping.head._1 == LiteMatBGPNode.CONCEPTS_TYPE)
        filters.append((
          LabelBase.SUBJECT_COLUMN_NAME,
          conceptsLMUdf(epMapping.head._2, epMapping.head._3)))
      if (epMapping.head._1 == LiteMatBGPNode.INDIVIDUALS_TYPE)
        filters.append((
          LabelBase.SUBJECT_COLUMN_NAME,
          individualsLMUdf(epMapping.head._2.toString)))
    } else {
      if (!this.bgpNode.triple.getSubject.isVariable)
        filters.append((
          LabelBase.SUBJECT_COLUMN_NAME,
          basicUdf(
            quoteString(this.bgpNode.triple.getSubject.toString())
          )))
    }

    /**
      * Construct the equivalent filter function on Predicate
      */
    if (epMapping(1)._1 == LiteMatBGPNode.PROPERTIES_TYPE) {
      filters.append((
        LabelBase.PREDICATE_COLUMN_NAME,
        propertiesLMUdf(epMapping(1)._2, epMapping(1)._3)))
    } else {
      if (!this.bgpNode.triple.getPredicate.isVariable &&
        (epMapping(2)._1 == LiteMatBGPNode.NOT_REWRITABLE))
        filters.append((
          LabelBase.PREDICATE_COLUMN_NAME,
          basicUdf(
            quoteString(this.bgpNode.triple.getPredicate.toString())
          )))
    }

    /**
      * Construct the equivalent filter function on Object
      */
    if (epMapping(2)._1 != LiteMatBGPNode.NOT_REWRITABLE) {
      if (epMapping(2)._1 == LiteMatBGPNode.CONCEPTS_TYPE)
        filters.append((
          LabelBase.OBJECT_COLUMN_NAME,
          conceptsLMUdf(epMapping(2)._2, epMapping(2)._3)))
      if (epMapping(2)._1 == LiteMatBGPNode.INDIVIDUALS_TYPE)
        filters.append((
          LabelBase.OBJECT_COLUMN_NAME,
          individualsLMUdf(epMapping(2)._2.toString)))
    } else {
      if (!this.bgpNode.triple.getObject.isVariable)
        filters.append((
          LabelBase.OBJECT_COLUMN_NAME,
          basicUdf(
            quoteString(this.bgpNode.triple.getObject.toString())
          )))
    }
    filters
  }

  def compute(inputDF: DataFrame): DataFrame = {
    computeLiteMatTriplePattern(rename(bgpNode, inputDF))
  }

  /**
    * The method uses LiteMat to compute each
    * triple pattern presents in current BGP
    *
    * @return The result (DataFrame) of the given triple pattern
    */
  def computeLiteMatTriplePattern(inputDF: DataFrame): DataFrame = {

    val outputDF = {
      if (liteMatFilters.isEmpty)
        super.computeTriplePattern(this.bgpNode.triple, inputDF)
      else {
        liteMatFilters.length match {
          case 1 =>
            val columnName_0 = liteMatFilters.head._1
            inputDF.filter(liteMatFilters.head._2(inputDF(s"$columnName_0")))

          case 2 =>
            val columnName_0 = liteMatFilters.head._1
            val columnName_1 = liteMatFilters(1)._1
            inputDF.filter(liteMatFilters.head._2(inputDF(s"$columnName_0"))).
              filter(liteMatFilters(1)._2(inputDF(s"$columnName_1")))

          case 3 =>
            val columnName_0 = liteMatFilters.head._1
            val columnName_1 = liteMatFilters(1)._1
            val columnName_2 = liteMatFilters(2)._1
            inputDF.filter(liteMatFilters.head._2(inputDF(s"$columnName_0"))).
              filter(liteMatFilters(1)._2(inputDF(s"$columnName_1"))).
              filter(liteMatFilters(2)._2(inputDF(s"$columnName_2")))
        }
      }
    }.select(this.outpuSchema.head, this.outpuSchema.tail: _*)

    outputDF
  }
}

object LiteMatBGPNode {
  private val CONCEPTS_TYPE = "CONCEPTS"
  private val PROPERTIES_TYPE = "PROPERTIES"
  private val INDIVIDUALS_TYPE = "INDIVIDUALS"
  private val NOT_REWRITABLE = "NOT_REWRITABLE"
}