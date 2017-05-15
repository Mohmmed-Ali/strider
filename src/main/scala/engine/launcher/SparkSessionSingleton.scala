package engine.launcher

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by xiangnanren on 02/01/2017.
  */
object SparkSessionSingleton {
  @transient private var sparkSession: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    sparkSession
  }
}