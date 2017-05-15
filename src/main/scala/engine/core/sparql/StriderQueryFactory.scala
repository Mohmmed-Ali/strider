package engine.core.sparql

import org.apache.jena.query.{Query, QueryFactory}
import org.apache.log4j.LogManager

/**
  * Created by xiangnanren on 25/11/2016.
  */
class StriderQueryFactory(queryStr: String) extends Serializable {
  @transient
  lazy val log = LogManager.getLogger(this.getClass)
  val query: Query = QueryFactory.create(queryStr)

  def createSelect(): SelectQuery = {
    if (query.isSelectType) SelectQuery(query)
    else
      throw InvalidQueryException("Invalid query, a select type query is required.")
  }

  def createConstruct(): ConstructQuery = {
    if (query.isConstructType) ConstructQuery(query)
    else
      throw InvalidQueryException("Invalid query, a construct type query is required.")
  }

  def createAsk(): AskQuery = {
    if (query.isConstructType) AskQuery(query)
    else
      throw InvalidQueryException("Invalid query, a ask type query is required.")
  }

  def createDescribe(): DescribeQuery = {
    if (query.isConstructType) DescribeQuery(query)
    else
      throw InvalidQueryException("Invalid query, a describe type query is required.")
  }


  def createQuery: SparqlQuery = query match {
    case _query if _query.isSelectType => SelectQuery(_query)

    case _query if _query.isConstructType => ConstructQuery(_query)

    case _query if _query.isAskType => AskQuery(_query)

    case _query if _query.isDescribeType => DescribeQuery(_query)

    case _ => throw InvalidQueryException("" +
      "Invalid query, input query should be one of the following types: " +
      "select, construct, ask or describe.")
  }

}


object StriderQueryFactory {
  def apply(queryStr: String): StriderQueryFactory = new StriderQueryFactory(queryStr)
}


