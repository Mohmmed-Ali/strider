package engine.core.conf

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext}

/**
  * Created by xiangnanren on 07/12/2016.
  */
abstract class CxtResolver

class StriderCxt(val batch: Duration,
                 val slide: Duration = Milliseconds(0L),
                 val window: Duration = Milliseconds(0L)) extends CxtResolver {
  val updateFrequency =
    if (slide.isZero) batch.milliseconds
    else slide.milliseconds

  def getStreamingCtx(striderConf: StriderConfBase): StreamingContext = {
    new StreamingContext(
      new StriderConfBase().conf, batch)
  }

  def getSparkSession(confBase: StriderConfBase,
                      threshold: String = "100000000"): SparkSession = {
    val spark = SparkSession.
      builder.
      config(confBase.conf).
      getOrCreate()

    spark.sqlContext.setConf("spark.sql.codegen", "false")
    spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")
    spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", threshold)
    spark.sqlContext.setConf("spark.sql.tungsten.enabled", "true")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    spark
  }
}

