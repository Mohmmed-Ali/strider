package engine.core.sparkop.op

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by xiangnanren on 11/07/16.
  */

class SparkOpRes(val result: DataFrame) {
  def saveAsFile(path: String): Unit = {
    result.rdd.map(x =>
      x(0) +
        " " + x(1) +
        " " + x(2) +
        " .\n").
      saveAsTextFile(path)
  }

  def showResult(): Unit = {
    result.show(20, false: Boolean)
  }
}

object SparkOpRes {
  def apply(result: DataFrame): SparkOpRes = new SparkOpRes(result)
}

case class SparkAskRes(override val result: DataFrame)
  extends SparkOpRes(result) {
  override def showResult(): Unit = {
    println(result.take(1).nonEmpty)
  }
}