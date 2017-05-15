package engine.core.sparkop.op

import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by xiangnanren on 11/07/16.
  */

abstract class SparkRes {

  def saveAsFile(path: String): Unit

  def showResult(): Unit
}

case class SparkOpRes(result: DataFrame) extends SparkRes {
  override def saveAsFile(path: String): Unit = {
    result.rdd.map(x =>
      x(0) +
        " " + x(1) +
        " " + x(2) +
        " .\n").
      saveAsTextFile(path)
  }

  override def showResult(): Unit = {
    result.show(20, false: Boolean)
  }
}

case class SparkConstructRes(result: RDD[Row]) extends SparkRes {
  override def saveAsFile(path: String): Unit = {
    result.map(x =>
      x(0) +
        " " + x(1) +
        " " + x(2) +
        " .\n").
      saveAsTextFile(path)
  }

  override def showResult(): Unit = {
    result.take(20).foreach(println(_))
  }
}

case class SparkAskRes(result: Boolean) extends SparkRes {
  override def saveAsFile(path: String): Unit = {
    val writer = new PrintWriter(new File(path))
    writer.write(result.toString)

    writer.close()
  }

  override def showResult(): Unit = {
    println(result)
  }
}

case class SparkDescribeRes(result: AnyRef) extends SparkRes {
  override def saveAsFile(path: String): Unit = ???

  override def showResult(): Unit = ???
}