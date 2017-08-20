package engine.core.sparql.reasoning

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable.ListBuffer


/**
  * Created by xiangnanren on 13/06/2017.
  */
sealed abstract class Decoder {
  def decode(inputDF: DataFrame): DataFrame

  def decode(inputRDD: RDD[Array[String]]): RDD[Array[String]]
}

case class LiteMatDecoder(dct: LiteMatEDCT) extends Decoder{
  def replaceRow(row: Row,
                 dct: LiteMatEDCT): Row = {
    val l = ListBuffer[String]()
    var c = 0

    while (c < row.length) {
      l.append(dct.conceptMapping.getOrElse(row.getString(c), row.getString(c)))
      l.append(dct.propertyMapping.getOrElse(row.getString(c), row.getString(c)))
      c += 1
    }
    Row(l:_*)
  }

  override def decode(inputDF: DataFrame): DataFrame = {
    implicit val encoder = RowEncoder(inputDF.schema)

    inputDF.mapPartitions(iter => {
      for (i <- iter) yield {
        replaceRow(i, dct)
      }
    })(encoder)
  }

  override def decode(inputRDD: RDD[Array[String]]): RDD[Array[String]] = ???

}
