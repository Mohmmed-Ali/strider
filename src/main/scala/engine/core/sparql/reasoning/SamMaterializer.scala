package engine.core.sparql.reasoning

import engine.stream.RDFTriple
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by xiangnanren on 19/08/2017.
  */

/**
  * The objet contains the utils for
  */
object SamMaterializer {

  def materializeRDDPair(matDct: Map[String, Array[String]] ,
                     inputRDD: RDD[(String, RDFTriple)]): RDD[RDFTriple] = {
    inputRDD.mapPartitions( iter => {
      for(i <- iter) yield {
        if (matDct.get(i._2.s).nonEmpty) {
          for(j <- matDct(i._2.s)) yield RDFTriple(i._2.s, "<http://www.w3.org/2002/07/owl#sameAs>", j)
        }
        else if (matDct.get(i._2.o).nonEmpty) {
          for(j <- matDct(i._2.o)) yield RDFTriple(i._2.o, "<http://www.w3.org/2002/07/owl#sameAs>", j)
        }
        else Array(i._2)
      }
    }.flatMap(x => x))
  }

  def materializeRDD(matDct: Map[String, Array[String]] ,
                   inputRDD: RDD[RDFTriple]): RDD[RDFTriple] = {
      inputRDD.mapPartitions( iter => {
        for(i <- iter) yield {
          if (matDct.get(i.s).nonEmpty) {
            for(j <- matDct(i.s)) yield RDFTriple(i.s, "<http://www.w3.org/2002/07/owl#sameAs>", j)
          }
          else if (matDct.get(i.o).nonEmpty) {
            for(j <- matDct(i.o)) yield RDFTriple(i.o, "<http://www.w3.org/2002/07/owl#sameAs>", j)
          }
          else Array(i)
        }
      }.flatMap(x => x))
    }

  def transformDct(filePath: String): Map[String, Array[String]] = {
    try {
      val in = classOf[LiteMatCtxHelper].getResourceAsStream(filePath)
      val iter = Source.fromInputStream(in, "utf-8").getLines().map(_.split(" "))
      val dct = (for (i <- iter) yield "<" + i(0) + ">" -> i(1)).toMap

      for (m <- dct) yield {
        val l = new ArrayBuffer[String]
        dct.foreach(n =>
          if (n._2 == m._2)
            l.append(n._1))

        m._1 -> l.toArray
      }

    } catch {
      case e: Exception =>
        throw new LiteMatException("Fail to load input dictionary of concepts")
    }
  }

}
