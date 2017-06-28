package engine.core.conf.syntaxparser

import engine.core.conf.StriderConfBase
import engine.core.sparkop.executor.QueryExecutor
import engine.core.sparql.{SparqlQuery, StriderQueryFactory}
import org.apache.spark.streaming._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParArray

/**
  * Created by xiangnanren on 21/02/2017.
  */
class StriderCtxHandler(striderConfig: ParsedStriderQuery)
  extends SyntaxParser {
  private[this] val streamingConfig = striderConfig.parsedStreamingConfig
  private[this] val registerConfig = striderConfig.parsedRegisterConfig

  def initStreamingCtx(striderConf: StriderConfBase): StreamingContext = {
    def initDuration(num: Long, durationType: String): Duration =
      durationType match {
        case MILLISECONDS.normalized => Milliseconds(num)
        case SECONDS.normalized => Seconds(num)
        case MINUTES.normalized => Minutes(num)
      }

    val batch = streamingConfig.get(BATCH.normalized)
    val duration = initDuration(batch._1, batch._2)

    new StreamingContext(striderConf.conf, duration)
  }


  def initQueryExecutorPool: ParArray[(SparqlQuery, QueryExecutor)] = {
    val taskNum = registerConfig.size

    val pool = registerConfig.get.map { m =>
      val sparqlQuery =
        StriderQueryFactory(m(SPARQL.normalized)).createQuery
      sparqlQuery -> new QueryExecutor(sparqlQuery)
    }.toParArray

    pool.tasksupport = new ForkJoinTaskSupport(
      new scala.
      concurrent.
      forkjoin.
      ForkJoinPool(taskNum))

    pool
  }
}

object StriderCtxHandler {
  def apply(striderConfig: ParsedStriderQuery): StriderCtxHandler =
    new StriderCtxHandler(striderConfig)
}