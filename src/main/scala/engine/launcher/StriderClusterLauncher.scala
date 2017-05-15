package engine.launcher

import engine.core.conf.{StriderConfBase, StriderCxt}
import engine.core.sparql.executor.SelectExecutor
import engine.core.label.LabelBase
import engine.core.sparql.{SparqlQueryInitializer, StriderQueryFactory}
import engine.stream.{KafkaStreamConsumer, MsgRDFTriple, RDFTriple, StreamDeserializer}
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by xiangnanren on 10/12/2016.
  */
object StriderClusterLauncher {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: <brokerAddr> <queryId>
           |  <brokerAddr> is a list of one or more Kafka brokers
           |  <brokerAddr> is a list of one or more Kafka topics to consume from
           |  <queryId> is the query Id of predefined queries
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokerAddr, queryId) = args

    val queryStr = SparqlQueryInitializer.
      initializeQueryStr(queryId)
    val query = StriderQueryFactory(queryStr).
      createSelect()

    val conf = new StriderConfBase()
    val ssc = new StriderCxt(Seconds(20L)).getStreamingCtx(conf)

    val kafkaStreamConsumer = new KafkaStreamConsumer(
      brokerAddr, "", "consumer-group-test", None, MsgRDFTriple.value)

    val kafkaStream = KafkaUtils.
      createDirectStream[
      String,
      RDFTriple,
      StringDecoder,
      StreamDeserializer[RDFTriple]](
      ssc,
      kafkaStreamConsumer.kafkaParams,
      kafkaStreamConsumer.topicsSet
    )

    kafkaStream.foreachRDD { rdd =>

      val spark = SparkSession.builder.
        config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      spark.sqlContext.setConf("spark.sql.codegen", "false")
      spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
      spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
      spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "170000000")
      spark.sqlContext.setConf("spark.sql.tungsten.enabled", "true")

      val t1 = System.nanoTime()

      val wdf = rdd.map(x => x._2).toDF(
        LabelBase.SUBJECT_COLUMN_NAME,
        LabelBase.PREDICATE_COLUMN_NAME,
        LabelBase.OBJECT_COLUMN_NAME
      ).persist(StorageLevel.MEMORY_ONLY)

      wdf.createOrReplaceTempView(LabelBase.INPUT_DATAFRAME_NAME)

      val res = SelectExecutor(query).executeSelect(wdf).result
      res.show(20, false: Boolean)

      val t2 = System.nanoTime()
      println("Costs: " + (t2 - t1) / 1e6)
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
