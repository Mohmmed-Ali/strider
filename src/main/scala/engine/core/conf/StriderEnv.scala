package engine.core.conf

import engine.core.conf.syntaxparser.ParsedStriderQuery

/**
  * Created by xiangnanren on 08/08/2017.
  */
class StriderEnv(queryStr: String,
                 conf: StriderConfBase,
                 shuffledPartitions: String,
                 threshold: String) {

  def this(queryStr: String, conf: StriderConfBase) =
    this(queryStr, conf, null, null)


  private val parsedQuery =  ParsedStriderQuery(queryStr)
  private val ctxHandler = StriderCtxHandler(parsedQuery)

  val executorPool = ctxHandler.initQueryExecutorPool
  val striderStreamingCtx = ctxHandler.initStriderCtx
  val sparkStreamingCtx = striderStreamingCtx.getStreamingCtx(conf)
  val sparkSession = ctxHandler.getSparkSession(conf, shuffledPartitions, threshold)
}
