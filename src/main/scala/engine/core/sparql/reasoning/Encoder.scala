package engine.core.sparql.reasoning

import engine.stream.RDFTriple
import org.apache.spark.rdd.RDD

/**
  * Created by xiangnanren on 13/06/2017.
  */

sealed abstract class Encoder extends Serializable

case class LiteMatEncoder(dct: LiteMatEDCT) extends Encoder {

  private val encodeSO:
  (String,
    Map[String, String],
    Map[String, String]) => String =
    (arg, cptMapping, indMapping) =>
      cptMapping.getOrElse(arg, indMapping.getOrElse(arg, arg))

  private val encodeP:
  (String, Map[String, String]) => String =
    (arg, propMapping)=> propMapping.getOrElse(arg, arg)

  def encodeBasicRDD(inputRDD: RDD[Array[String]]): RDD[Array[String]] = {
    inputRDD.mapPartitions(iter =>
      for (i <- iter) yield Array(
        encodeSO(i(0), dct.conceptMapping, dct.individualMapping),
        encodeP(i(1), dct.propertyMapping),
        encodeSO(i(2), dct.conceptMapping, dct.individualMapping)))
  }

  def encodeRDFTripleRDD(inputRDD: RDD[RDFTriple]): RDD[RDFTriple] = {
    inputRDD.mapPartitions(iter =>
      for(i <- iter) yield RDFTriple(
        encodeSO(i.s, dct.conceptMapping, dct.individualMapping),
        encodeP(i.p, dct.propertyMapping),
        encodeSO(i.o, dct.conceptMapping, dct.individualMapping)
      ))
  }

  def encodeRDFTripleRDDPair(inputRDD: RDD[(String, RDFTriple)]): RDD[RDFTriple] = {
    inputRDD.mapPartitions(iter =>
      for (i <- iter) yield RDFTriple(
        encodeSO(i._2.s, dct.conceptMapping, dct.individualMapping),
        encodeP(i._2.p, dct.propertyMapping),
        encodeSO(i._2.o, dct.conceptMapping, dct.individualMapping)
      ))
  }

  def encodeSource(message: RDFTriple): RDFTriple = RDFTriple(
    encodeSO(message.s, dct.conceptMapping, dct.individualMapping),
    encodeP(message.p, dct.propertyMapping),
    encodeSO(message.p, dct.conceptMapping, dct.individualMapping))

}
