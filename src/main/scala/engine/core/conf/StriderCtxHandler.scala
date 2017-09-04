package engine.core.conf

import engine.core.conf.syntaxparser.{ParsedStriderQuery, SyntaxParser}
import engine.core.sparkop.executor.QueryExecutor
import engine.core.sparql.{SparqlQuery, StriderQueryFactory}
import org.apache.spark.streaming._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParArray

/**
  * Created by xiangnanren on 21/02/2017.
  */
class StriderCtxHandler(striderConfig: ParsedStriderQuery)
  extends SyntaxParser with StriderCxtResolver {
  private[this] val streamingConfig = striderConfig.parsedStreamingConfig
  private[this] val registerConfig = striderConfig.parsedRegisterConfig

  def initStriderCtx: StriderStreamingCxt = {
    def initDuration(num: Long, durationType: String): Duration =
      durationType match {
        case MILLISECONDS.normalized => Milliseconds(num)
        case SECONDS.normalized => Seconds(num)
        case MINUTES.normalized => Minutes(num)
      }

    val batch = streamingConfig.get(BATCH.normalized)
    val batchDuration = initDuration(batch._1, batch._2)
    val windowDuration = streamingConfig.get.get(WINDOW.normalized)
    val slideDuration = streamingConfig.get.get(SLIDE.normalized)

    if (windowDuration.nonEmpty && slideDuration.nonEmpty)
      new StriderStreamingCxt(batchDuration,
        initDuration(windowDuration.get._1, windowDuration.get._2),
        initDuration(slideDuration.get._1, slideDuration.get._2))
    else new StriderStreamingCxt(batchDuration)
  }

  def initQueryExecutorPool: ParArray[(SparqlQuery, QueryExecutor)] = {
    val taskNum = registerConfig.size

    val pool = registerConfig.get.map { m =>
      val sparqlQuery =
        StriderQueryFactory(
          m(SPARQL.normalized),
          m(QUERYID.normalized),
          m(REASONING.normalized).toBoolean).
          createQuery

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