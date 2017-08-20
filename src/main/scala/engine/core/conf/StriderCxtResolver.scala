package engine.core.conf

import org.apache.spark.sql.SparkSession

/**
  * Created by xiangnanren on 07/12/2016.
  */
trait StriderCxtResolver {
  def getSparkSession(confBase: StriderConfBase,
                      shuffledPartitions: String = "8",
                      threshold: String = "-1"): SparkSession = {
    val spark = SparkSession.
      builder.
      config(confBase.conf).
      getOrCreate()

    spark.sqlContext.setConf("spark.sql.codegen", "false")
    spark.sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")
    spark.sqlContext.setConf("spark.sql.tungsten.enabled", "true")
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", shuffledPartitions)
    spark.sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", threshold)

    spark
  }
}
