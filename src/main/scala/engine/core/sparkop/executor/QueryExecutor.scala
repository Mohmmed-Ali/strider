package engine.core.sparkop.executor

import java.io.{File, PrintWriter}

import engine.core.label.LabelBase
import engine.core.sparkop.compiler.{SparkOpPrinter, SparkOpUpdater}
import engine.core.sparkop.op.{SparkAskRes, SparkOpRes}
import engine.core.sparql._
import engine.core.sparql.reasoning.LiteMatOpExecutor
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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
  */
sealed class QueryExecutor(@transient val query: SparqlQuery) {
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

  def execute(inputDF: DataFrame): SparkOpRes = {
    query match {
      case _query: SelectQuery if _query.getClass == classOf[SelectQuery] =>
        new SelectExecutor(_query).execute(inputDF)
      case _query: LiteMatSelectQuery if _query.getClass == classOf[LiteMatSelectQuery] =>
        new LiteMatSelectExecutor(_query).execute(inputDF)
      case _query: ConstructQuery if _query.getClass == classOf[ConstructQuery] =>
        new ConstructExecutor(_query).execute(inputDF)
      case _query: LiteMatConstructQuery if _query.getClass == classOf[LiteMatConstructQuery] =>
        new LiteMatConstructExecutor(_query).execute(inputDF)
      case _query: AskQuery if _query.getClass == classOf[AskQuery] =>
        new AskExecutor(_query).execute(inputDF)
      case _query: LiteMatAskQuery if _query.getClass == classOf[LiteMatAskQuery] =>
        new LiteMatAskExecutor(_query).execute(inputDF)
    }
  }

  protected def executeAlgebra(inputDF: DataFrame): SparkOpRes = {
    SparkOpExecutor(inputDF).execute(sparkOpRoot)
  }

  protected def executeLiteMatAlgebra(inputDF: DataFrame): SparkOpRes = {
    LiteMatOpExecutor(inputDF).execute(sparkOpRoot)
  }
}

object QueryExecutor {

  def apply(@transient query: SparqlQuery): QueryExecutor = query match {
    case _query: SparqlQuery
      if query.getClass == classOf[SparqlQuery] =>
      new QueryExecutor(_query)
  }

  def apply(@transient query: SelectQuery): SelectExecutor = query match {
    case _query: SelectQuery
      if query.getClass == classOf[SelectQuery] =>
      new SelectExecutor(_query)
  }

  def apply(@transient query: LiteMatSelectQuery): LiteMatSelectExecutor = query match {
    case _query: LiteMatSelectQuery
      if query.getClass == classOf[LiteMatSelectQuery] =>
      new LiteMatSelectExecutor(_query)
  }

  def apply(@transient query: ConstructQuery): ConstructExecutor = query match {
    case _query: ConstructQuery
      if query.getClass == classOf[ConstructQuery] =>
      new ConstructExecutor(_query)
  }

  def apply(@transient query: LiteMatConstructQuery): LiteMatConstructExecutor = query match {
    case _query: LiteMatConstructQuery
      if query.getClass == classOf[LiteMatConstructQuery] =>
      new LiteMatConstructExecutor(_query)
  }

  def apply(@transient query: AskQuery): AskExecutor = query match {
    case _query: AskQuery
      if query.getClass == classOf[AskQuery] =>
      new AskExecutor(_query)
  }

  def apply(@transient query: LiteMatAskQuery): LiteMatAskExecutor = query match {
    case _query: LiteMatAskQuery
      if query.getClass == classOf[LiteMatAskQuery] =>
      new LiteMatAskExecutor(_query)
  }
}


/**
  * Executor for select-type query
  *
  * @param query : input Sparql query
  */
class SelectExecutor(@transient override val query: SelectQuery) extends
  QueryExecutor(query) {
  override def execute(inputDF: DataFrame): SparkOpRes = {
    executeAlgebra(inputDF)
  }
}


/**
  * Executor for select-type query with applying LiteMat reasoning
  *
  * @param query : input Sparql query
  */
class LiteMatSelectExecutor(@transient override val query: LiteMatSelectQuery)
  extends SelectExecutor(query) {
  override def execute(inputDF: DataFrame): SparkOpRes = {
    executeLiteMatAlgebra(inputDF)
  }
}


/**
  * Executor for construct-type query
  *
  * @param query : input Sparql query
  */
class ConstructExecutor(@transient override val query: ConstructQuery) extends
  QueryExecutor(query) {
  private val fields = Array(s"${LabelBase.SUBJECT_COLUMN_NAME}",
    s"${LabelBase.PREDICATE_COLUMN_NAME}",
    s"${LabelBase.OBJECT_COLUMN_NAME}").
    map(fieldName =>
      StructField(fieldName, StringType, nullable = true))
  final protected val schema = StructType(fields)
  final protected val encoder = RowEncoder(schema)
  final protected val templateMapping = query.templateMapping

  /**
    * Method to trigger the execution of construct clause,
    * it returns the result as a new RDD of row.
    */
  override def execute(inputDF: DataFrame): SparkOpRes = {

    val res = executeAlgebra(inputDF).result
    val resConstruct = res.mapPartitions(iter =>
      (for (i <- iter)
        yield constructByTemplate(i, templateMapping)).
        flatMap(rs => rs))(this.encoder)
    SparkOpRes(resConstruct)
  }

  /**
    * Transform original DataFrame to new DataFrame row by row with the
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
  protected def constructByTemplate(row: Row,
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


/**
  * Executor for construct-type query with applying LiteMat reasoning
  *
  * @param query : input Sparql query
  */
class LiteMatConstructExecutor(@transient override val query: LiteMatConstructQuery)
  extends ConstructExecutor(query) {

  override def execute(inputDF: DataFrame): SparkOpRes = {
    val res = executeLiteMatAlgebra(inputDF).result
    val resConstruct = res.mapPartitions(iter =>
      (for (i <- iter)
        yield constructByTemplate(i, templateMapping)).
        flatMap(rs => rs))(this.encoder)
    SparkOpRes(resConstruct)
  }
}


/**
  * Executor for ask-type query
  *
  * @param query : input Sparql query
  */
class AskExecutor(@transient override val query: AskQuery) extends
  QueryExecutor(query) {

  override def execute(inputDF: DataFrame): SparkAskRes =
    SparkAskRes(
      executeAlgebra(inputDF).result)
}


/**
  * Executor for ask-type query with applying LiteMat Reasoning
  *
  */
class LiteMatAskExecutor(@transient override val query: LiteMatAskQuery)
  extends AskExecutor(query) {
  override def execute(inputDF: DataFrame): SparkAskRes = {
    SparkAskRes(
      executeLiteMatAlgebra(inputDF).result)
  }
}
