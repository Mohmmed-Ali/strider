package engine.core.sparkop.executor

import engine.core.sparql.{AskQuery, ConstructQuery, SelectQuery}

/**
  * Created by xiangnanren on 23/06/2017.
  */
object QueryImpConversion {
  implicit def toSelectExecutor(qe: QueryExecutor): SelectExecutor =
    SelectExecutor(qe.query.asInstanceOf[SelectQuery])

  implicit def toConstructExecutor(qe: QueryExecutor): ConstructExecutor =
    ConstructExecutor(qe.query.asInstanceOf[ConstructQuery])

  implicit def toAskQuery(qe: QueryExecutor): AskExecutor =
    AskExecutor(qe.query.asInstanceOf[AskQuery])
}
