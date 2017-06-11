package engine.core.sparkop.executor

import java.io.{File, PrintWriter}

import engine.core.sparkop.compiler.{SparkOpPrinter, SparkOpUpdater}
import engine.core.sparkop.op.{SparkAskRes, SparkConstructRes, SparkOpRes, SparkRes}
import engine.core.sparql._
import org.apache.spark.sql.{DataFrame, Row}


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
  * @tparam T : upper bound of input query type
  */
class QueryExecutor[T <: SparqlQuery](@transient val query: T) {
  @transient
  private val sparkOpRoot = query.sparkOpRoot

  def updateAlgebra(inputDF: DataFrame): Unit = {
    SparkOpUpdater(inputDF).update(sparkOpRoot)
  }

  def executeQuery(inputDF: DataFrame): SparkRes = {
    query match {
      case _query: SelectQuery =>
        SelectExecutor(_query).executeSelect(inputDF)
      case _query: ConstructQuery =>
        ConstructExecutor(_query).executeConstruct(inputDF)
      case _query: AskQuery =>
        AskExecutor(_query).executeAsk(inputDF)
      case _query: DescribeQuery =>
        DescribeExecutor(_query).executeDescribe(inputDF)
    }
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
}

object QueryExecutor {
  def apply[T <: SparqlQuery](@transient query: T):
  QueryExecutor[T] = new QueryExecutor[T](query)
}


case class SelectExecutor(@transient override val query: SelectQuery) extends
  QueryExecutor[SelectQuery](query) {

  def executeSelect(inputDF: DataFrame): SparkOpRes = {
    executeAlgebra(inputDF)
  }
}


case class ConstructExecutor(@transient override val query: ConstructQuery) extends
  QueryExecutor[ConstructQuery](query) {
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
  QueryExecutor[AskQuery](query) {

  def executeAsk(inputDF: DataFrame): SparkAskRes =
    SparkAskRes(
      executeAlgebra(inputDF).result.take(1).nonEmpty)

}

// To do
case class DescribeExecutor(@transient override val query: DescribeQuery) extends
  QueryExecutor[DescribeQuery](query) {

  def executeDescribe(inputDF: DataFrame): SparkOpRes = ???
}