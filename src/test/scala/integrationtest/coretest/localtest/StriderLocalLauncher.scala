package integrationtest.coretest.localtest

import java.util.Calendar

import engine.core.conf.{StriderConfBase, StriderCxt}
import engine.core.label.LabelBase
import engine.core.sparql.executor.SelectExecutor
import engine.core.sparql.{SparqlQueryInitializer, StriderQueryFactory}
import engine.launcher.SparkSessionSingleton
import engine.stream.{KafkaStreamConsumer, MsgRDFTriple, RDFTriple, StreamDeserializer}
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.parallel.ForkJoinTaskSupport

/**
  * Created by xiangnanren on 08/12/2016.
  */

/**
  * In local mode, the console may show the following warnings:
  *
  * "Block rdd_i already exists on this machine; not re-adding it"
  *
  * Spark Streaming is designed to replicate the received data within the
  * machines in a Spark cluster for fault-tolerance. However, when you are
  * running in the local mode, since there is only one machine, the
  * "blocks" of data are not able to replicate. This is expected and safe to
  * ignore in local mode.
  */
object StriderLocalLauncher {

  val queryStr = SparqlQueryInitializer.initializeQueryStr()
  val query = StriderQueryFactory(queryStr).createSelect()

  def main(args: Array[String]) {

    val conf = new StriderConfBase
    val ssc = new StriderCxt(Seconds(20L)).getStreamingCtx(conf)

    val kafkaStreamConsumer = new KafkaStreamConsumer(
      "localhost:9092", "", "consumer-group-test", None, MsgRDFTriple.value)

    val concurrencyPool = (1 to 1).toParArray
    concurrencyPool.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(1))

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

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")
      spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "170000000")
      spark.sqlContext.setConf("spark.sql.tungsten.enabled", "true")

      println("Start moment: " + Calendar.getInstance().getTime)
      val tStart = System.nanoTime()

      val wdf = rdd.map(x => x._2).toDF(
        LabelBase.SUBJECT_COLUMN_NAME,
        LabelBase.PREDICATE_COLUMN_NAME,
        LabelBase.OBJECT_COLUMN_NAME
      ).persist(StorageLevel.MEMORY_ONLY)

      val inputRows = wdf.count()

      concurrencyPool.foreach { query_id =>
        val t1 = System.nanoTime()
        val res = SelectExecutor(query).executeSelect(wdf).result
        val numRes = res.count()
        val t2 = System.nanoTime()

        res.show(10, false: Boolean)

        println("---Query Id: " + query_id +
          ", the number of rows: " +
          numRes + ", Computing Costs: " +
          (t2 - t1) / 1e6 +
          ", input DF rows: " + inputRows +
          " ---")
      }

      val tEnd = System.nanoTime()
      println(1 + " queries execution costs: " + (tEnd - tStart) / 1e6)
      println("End moment: " + Calendar.getInstance().getTime)
      println()

      spark.sqlContext.clearCache()
      rdd.unpersist(true)
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
