package engine.core.sparkop.executor

import engine.core.sparql._

/**
  * Created by xiangnanren on 23/06/2017.
  */
object QueryExecutorImpConversion {
  implicit def toSelectExecutor(qe: QueryExecutor): SelectExecutor =
    new SelectExecutor(qe.query.asInstanceOf[SelectQuery])
  implicit def toLiteMatSelectExecutor(qe: QueryExecutor): LiteMatSelectExecutor =
    new LiteMatSelectExecutor(qe.query.asInstanceOf[LiteMatSelectQuery])
  implicit def toConstructExecutor(qe: QueryExecutor): ConstructExecutor =
    new ConstructExecutor(qe.query.asInstanceOf[ConstructQuery])
  implicit def toLiteMatConstructExecutor(qe: QueryExecutor): LiteMatConstructExecutor =
    new LiteMatConstructExecutor(qe.query.asInstanceOf[LiteMatConstructQuery])
  implicit def toAskExecutor(qe: QueryExecutor): AskExecutor =
    new AskExecutor(qe.query.asInstanceOf[AskQuery])
  implicit def toLiteMatAsk(qe: QueryExecutor): LiteMatAskExecutor = {
    new LiteMatAskExecutor(qe.query.asInstanceOf[LiteMatAskQuery])
  }


//  implicit def toLiteMatConstructExecutor(qe: QueryExecutor): LiteMatConstructExecutor

}
