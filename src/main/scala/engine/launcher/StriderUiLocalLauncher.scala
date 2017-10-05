package engine.launcher

import java.util.Calendar

import engine.core.conf.{StriderConfBase, StriderEnv}
import engine.core.label.LiteMatConfLabels
import engine.core.sparql.{QueryReader, StriderQueryFactory}
import engine.core.sparql.reasoning.{LiteMatCtx, LiteMatEncoder}
import engine.stream.kryo.KryoStreamDeserializer
import engine.stream.{KafkaStreamConsumer, MessageConverter, MsgRDFTriple, RDFTriple}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by xiangnanren on 11/08/2017.
  */
object StriderUiLocalLauncher {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage:
           |  <brokerAddr> is a list of one or more Kafka brokers
           |  <query> is the query Id of predefined query
           |  <broadcastThreshold> the threshold for broadcast join
           |  <shuffledPartitions> number of the partitions for join operation
           |  <repartitionNum> number of partitions if repartition is required
           |  <concurrentStreamingJobs> concurrent spark streaming jobs
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokerAddr, query, broadcastThreshold,
    shuffledPartitions, repartitionNum,
    concurrentStreamingJobs) = args

//    val Array(brokerAddr, query, broadcastThreshold,
//    shuffledPartitions, repartitionNum,
//    concurrentStreamingJobs) = Array("localhost:9092",
//      " STREAMING {BATCH [5 SECONDS]} " +
//        " REGISTER { QUERYID [QUERY_1] " +
//        " REASONING [FALSE] " +
//        " SPARQL [ select ?s ?o6 " +
//      " { " +
//      " ?s  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o ; " +
//      "      <http://purl.oclc.org/NET/ssnx/ssn/isProducedBy> ?o1 ; " +
//        "    <http://purl.oclc.org/NET/ssnx/ssn/hasValue>    ?o2 ." +
//        " ?o2    <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>   ?o3 ; " +
//        "        <http://purl.oclc.org/NET/ssnx/ssn/startTime>    ?o4 ; " +
//        "        <http://data.nasa.gov/qudt/owl/qudt/unit>    ?o5 ; " +
//        "        <http://data.nasa.gov/qudt/owl/qudt/numericValue>    ?o6 ." +
//      "}] " +
//    "}",
//      "-1", "4", "8","1")

    val queryStr = new QueryReader().readQueryString(query)
    val conf = new StriderConfBase(concurrentStreamingJobs)
    val env = new StriderEnv(queryStr,
      conf, shuffledPartitions, broadcastThreshold)
    val liteMatEncoder = LiteMatEncoder(LiteMatCtx.EDCT)

    val kafkaStreamConsumer = new KafkaStreamConsumer(
      brokerAddr,
      "",
      "consumer-group-test",
      None,
      MsgRDFTriple.defaultTopic)

    val kafkaStream = KafkaUtils.
      createDirectStream[
      String,
      RDFTriple,
      StringDecoder,
      KryoStreamDeserializer[RDFTriple]](
      env.sparkStreamingCtx,
      kafkaStreamConsumer.kafkaParams,
      kafkaStreamConsumer.topicsSet
    )

   val repartitionedStream =
     if (repartitionNum.toInt != 0 )
       kafkaStream.repartition(repartitionNum.toInt)
     else kafkaStream

    repartitionedStream.foreachRDD{ rdd =>
      env.executorPool.foreach( t => {
        val df = MessageConverter.
          RDFTripleToDF(
            env.sparkSession, rdd,
            t._1, liteMatEncoder)

        println(s"######### The Outputs of [${t._1.id}]: " +
          s"@ ${Calendar.getInstance.getTime} #########")
        val res =t._2.execute(df).result
        res.show(10, false: Boolean)

      })
    }

    env.sparkStreamingCtx.start()
    env.sparkStreamingCtx.awaitTermination()
    env.sparkStreamingCtx.stop()
  }

}
