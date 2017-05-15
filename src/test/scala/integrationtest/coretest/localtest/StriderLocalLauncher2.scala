package integrationtest.coretest.localtest

import engine.core.conf.{StriderConfBase, StriderCxt}
import engine.core.optimizer.AdapStrategy.{Backward, Forward}
import engine.core.optimizer.trigger.TriggerRules
import engine.core.sparql.executor.SelectExecutor
import engine.core.sparql.{SparqlQueryInitializer, StriderQueryFactory}
import engine.stream._
import engine.util.Helper
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by xiangnanren on 21/03/2017.
  */

/**
  * Local launcher with mixed adaptive strategy
  */
object StriderLocalLauncher2 {
  val queryStr = SparqlQueryInitializer.initializeQueryStr()
  val query = StriderQueryFactory(queryStr).createSelect()
  val executor = SelectExecutor(query)

  def main(args: Array[String]) {
    val conf = new StriderConfBase
    val striderCtx = new StriderCxt(Seconds(2L))
    val streamingCtx = striderCtx.getStreamingCtx(conf)
    val sparkSession = striderCtx.getSparkSession(conf)

    val kafkaStreamConsumer = new KafkaStreamConsumer(
      "localhost:9092",
      "",
      "consumer-group-test",
      None,
      MsgRDFTriple.value)

    val kafkaStream = KafkaUtils.
      createDirectStream[
      String,
      RDFTriple,
      StringDecoder,
      StreamDeserializer[RDFTriple]](
      streamingCtx,
      kafkaStreamConsumer.kafkaParams,
      kafkaStreamConsumer.topicsSet
    )

    kafkaStream.foreachRDD { rdd =>
      val wdf = MessageConverter.RDFTripleToDF(sparkSession,rdd)

      val taskTime = Helper.getRunTime {
        val res = executor.executeSelect(wdf).result
        res.show(7,false: Boolean)
        println("The cardinality of outputs: " + res.count())
      }

      TriggerRules.f_switchStrategy(
        taskTime, striderCtx.updateFrequency) match {
        case Backward => executor.updateAlgebra(wdf)
        case Forward =>
      }

      sparkSession.sqlContext.clearCache()
      rdd.unpersist(true)
    }

    streamingCtx.start()
    streamingCtx.awaitTermination()
    streamingCtx.stop()
  }

}
