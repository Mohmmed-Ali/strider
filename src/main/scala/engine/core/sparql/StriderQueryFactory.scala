package engine.core.sparql

import engine.core.sparql.reasoning.LiteMatCtxBuilder
import org.apache.jena.query.{Query, QueryFactory}
import org.apache.log4j.LogManager

/**
  * Created by xiangnanren on 25/11/2016.
  */
class StriderQueryFactory(queryStr: String,
                          reasoningEnabled: Boolean = false)
  extends LiteMatCtxBuilder with Serializable  {
  @transient
  lazy val log = LogManager.getLogger(this.getClass)
  val query: Query = QueryFactory.create(queryStr)

  def setLiteMatArgs(key: String,
                     value: String): this.type = {
    set(key, value)
    this
  }

  def createSelect: SelectQuery = {
    if (query.isSelectType && !this.reasoningEnabled) new SelectQuery(query)
    else throw InvalidQueryException("Invalid query, a select type query is required.")
  }

  def createLiteMatSelect: LiteMatSelectQuery = {
    if (query.isSelectType && this.reasoningEnabled) new LiteMatSelectQuery(query)
    else throw InvalidQueryException("Invalid query, a LiteMat-Select type query is required.")
  }

  def createConstruct: ConstructQuery = {
    if (query.isConstructType && !this.reasoningEnabled) new ConstructQuery(query)
    else throw InvalidQueryException("Invalid query, a construct type query is required.")
  }

  def createLiteMatConstruct: LiteMatConstructQuery = {
    if (query.isConstructType && this.reasoningEnabled) new LiteMatConstructQuery(query)
    else throw InvalidQueryException("Invalid query, a LiteMat-Construct type query is required.")
  }

  def createAsk: AskQuery = {
    if (query.isConstructType && !this.reasoningEnabled) new AskQuery(query)
    else throw InvalidQueryException("Invalid query, a ask type query is required.")
  }

  def createLiteMatAsk: LiteMatAskQuery = {
    if (query.isAskType && this.reasoningEnabled) new LiteMatAskQuery(query)
    else throw InvalidQueryException("Invalid query, a LiteMat-Ask type query is required.")
  }


  def createQuery: SparqlQuery = query match {
    case _query if _query.isSelectType && !this.reasoningEnabled => new SelectQuery(_query)
    case _query if _query.isConstructType && !this.reasoningEnabled => new ConstructQuery(_query)
    case _query if _query.isAskType && !this.reasoningEnabled => new AskQuery(_query)
    case _query if _query.isSelectType && this.reasoningEnabled => new LiteMatSelectQuery(_query)
    case _query if _query.isConstructType && this.reasoningEnabled => new LiteMatConstructQuery(_query)
    case _query if _query.isAskType && this.reasoningEnabled => new LiteMatAskQuery(_query)
    case _ => throw InvalidQueryException("" +
      "Invalid query, input query should be one of the following types: " +
      "select, construct, or ask.")
  }

}


object StriderQueryFactory {
  def apply(queryStr: String,
            reasoningEnabled: Boolean = false): StriderQueryFactory =
    new StriderQueryFactory(queryStr, reasoningEnabled)
  def apply(queryStr: String): StriderQueryFactory =
    new StriderQueryFactory(queryStr)
}


