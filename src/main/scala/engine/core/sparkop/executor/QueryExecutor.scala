package engine.core.sparkop.executor

import java.io.{File, PrintWriter}

import engine.core.reasoning.LiteMatOpExecutor
import engine.core.sparkop.compiler.{SparkOpPrinter, SparkOpUpdater}
import engine.core.sparkop.op.{SparkAskRes, SparkConstructRes, SparkOpRes}
import engine.core.sparql._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, Row}

import scala.language.implicitConversions


/**
  * Created by xiangnanren on 25/11/2016.
  */

/**
  * The class for the query execution. QueryExecutor accepts two arguments:
  * input query (SparqlQuery) and the input data (DataFrame). The 'query'
  * should be annotated as transient, since its superclass SparqlQuery is
  * not serializable.
  *
  * @param query : input Sparql query
  */
class QueryExecutor(@transient val query: SparqlQuery) {
  @transient
  protected val sparkOpRoot = query.sparkOpRoot

  def updateAlgebra(inputDF: DataFrame): Unit = {
    SparkOpUpdater(inputDF).update(sparkOpRoot)
  }

  def writeAlgebra(path: String): Unit = {
    @transient
    lazy val logWriter = new PrintWriter(new File(path))

    logWriter.println("##########################")
    logWriter.println("### The query algebra: ###")
    logWriter.println("##########################\n" + sparkOpRoot)
    logWriter.println("#################################")
    logWriter.println("### The algebra of SPARK SQL: ###")
    logWriter.println("#################################")
    SparkOpPrinter(logWriter).printAlgebra(sparkOpRoot)

    logWriter.close()
  }

  protected def executeAlgebra(inputDF: DataFrame): SparkOpRes = {
    SparkOpExecutor(inputDF).execute(sparkOpRoot)
  }

  protected def executeLiteMatAlgebra(inputDF: DataFrame): SparkOpRes = {
    LiteMatOpExecutor(inputDF).execute(sparkOpRoot)
  }
}

object QueryExecutor {
  def apply(@transient query: SparqlQuery): QueryExecutor =
    new QueryExecutor(query)

  def apply(@transient query: SelectQuery): QueryExecutor =
    new SelectExecutor(query)

  def apply(@transient query: ConstructQuery): QueryExecutor =
    new ConstructExecutor(query)

  def apply(@transient query: AskQuery): QueryExecutor =
    new AskExecutor(query)

}


case class SelectExecutor(@transient override val query: SelectQuery) extends
  QueryExecutor(query) {

  def executeSelect(inputDF: DataFrame): SparkOpRes = {
    executeAlgebra(inputDF)
  }

  def executeLiteMatSelect(inputDF: DataFrame): SparkOpRes = {
    executeLiteMatAlgebra(inputDF)
  }
}


case class ConstructExecutor(@transient override val query: ConstructQuery) extends
  QueryExecutor(query) {
  private val templateMapping = query.templateMapping

  /**
    * Method to trigger the execution of construct clause,
    * it returns the result as a new RDD of row.
    */
  def executeConstruct(inputDF: DataFrame): SparkConstructRes = {

    val res = executeAlgebra(inputDF).result
    val resConstruct = res.rdd.map(rddRow =>
      constructByTemplate(rddRow, templateMapping)).
      flatMap(rows => rows)

    SparkConstructRes(resConstruct)
  }

  @Experimental
  def executeLiteMatConstruct(inputDF: DataFrame): SparkConstructRes = {

    val res = executeLiteMatAlgebra(inputDF).result
    val resConstruct = res.rdd.map(rddRow =>
      constructByTemplate(rddRow, templateMapping)).
      flatMap(rows => rows)

    SparkConstructRes(resConstruct)
  }


  /**
    * Transform original DataFrame to new RDD row by row with the
    * respect to the definition of the given construct clause
    * E.g.:
    *
    * row 1 -> as template -> (row 11, row 12...)
    * row 2 -> as template -> (row 21, row 22 ...)
    * ...
    *
    * @param row      : the row of algebra-part DataFrame
    * @param template : construct template
    */
  private def constructByTemplate(row: Row,
                                  template: List[TripleMapping]): List[Row] = {
    template.map(triple =>
      Row(lookup(row, triple.subjectNode),
        lookup(row, triple.predicateNode),
        lookup(row, triple.objectNode) + " ."))
  }

  /** Lookup the value to be filled up in the new RDD from
    * the already-computed DataFrame with the respect to
    * the construct template. If the node is variable, the method
    * reveals the value directly from the DataFrame and assign it
    * to the specified position of Row (or the method assign the
    * mapping of the node).
    *
    * @param row  : row in original DataFrame
    * @param node : node mapping of triples in construct clause
    */
  private def lookup(row: Row, node: NodeMapping): String = {
    node.isVariable match {
      case true => row.getAs(node.mapping)
      case false => node.mapping
    }
  }
}

case class AskExecutor(@transient override val query: AskQuery) extends
  QueryExecutor(query) {

  def executeAsk(inputDF: DataFrame): SparkAskRes =
    SparkAskRes(
      executeAlgebra(inputDF).result.take(1).nonEmpty)

  def executeLiteMatAsk(inputDF: DataFrame): SparkAskRes = {
    SparkAskRes(
      executeLiteMatAlgebra(inputDF).result.take(1).nonEmpty)
  }

}

