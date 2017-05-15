package engine.core.sparql

/**
  * Created by xiangnanren on 16/02/2017.
  */
object QueryImpConversion {
  implicit def SparqlToSelect(sparqlQuery: SparqlQuery): SelectQuery = {
    SelectQuery(sparqlQuery.query)
  }

  implicit def SparqlToConstruct(sparqlQuery: SparqlQuery): ConstructQuery = {
    ConstructQuery(sparqlQuery.query)
  }

  implicit def SparqlToAsk(sparqlQuery: SparqlQuery): AskQuery = {
    AskQuery(sparqlQuery.query)
  }
}
