package engine.stream

import engine.core.label.LabelBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.reflect.{ClassTag, classTag}


/**
  * Created by xiangnanren on 17/11/2016.
  */
object MessageConverter {
  /**
    * A method converts input RDF stream into DataFrame.
    * Input RDF stream should be the type of RDFTriple, RDFGraph or WavesEvent.
    *
    * Intellij gives a syntax error highlight for the following codes. The IDE
    * dose not recognize _rdd as RDD[RDFTriple/RDFGraph/WavesEvent] but RDD[T].
    * However the codes compile and run correctly.
    *
    * Be aware that this method requires a runtime reflection, which may give
    * an impact on performance. Try to use the more specific method (RDFTripleToDF/
    * RDFGraphToDF/WavesEventToDF) to create DataFrame if it is possible.
    *
    * @param sparkSession : A existing singleton spark session.
    * @param rdd          : the micro-batch rdd in DStream.
    * @tparam T : type parameter of RDFData.
    * @return The initial DataFrame which is DataFrame from input DStream.
    */

  def RDFToDF[T: ClassTag](sparkSession: SparkSession,
                           rdd: RDD[(String, T)]): DataFrame = {
    import sparkSession.implicits._

    rdd.mapPartitions(x => for (i <- x) yield i._2) match {
      case _rdd: RDD[RDFTriple@unchecked]
        if classTag[T] == classTag[RDFTriple] =>
        _rdd.toDF (
          LabelBase.SUBJECT_COLUMN_NAME,
          LabelBase.PREDICATE_COLUMN_NAME,
          LabelBase.OBJECT_COLUMN_NAME
        ).persist (StorageLevel.MEMORY_ONLY)
    }
  }

  def RDFTripleToDF(sparkSession: SparkSession,
                    rdd: RDD[RDFTriple]): DataFrame = {
    import sparkSession.implicits._

    rdd.toDF(
      LabelBase.SUBJECT_COLUMN_NAME,
      LabelBase.PREDICATE_COLUMN_NAME,
      LabelBase.OBJECT_COLUMN_NAME
    ).persist(StorageLevel.MEMORY_ONLY)
  }

  def RDFTriplePairToDF(sparkSession: SparkSession,
                    rdd: RDD[(String, RDFTriple)]): DataFrame = {
    import sparkSession.implicits._

    rdd.mapPartitions(x => for (i <- x) yield i._2).
      toDF(
        LabelBase.SUBJECT_COLUMN_NAME,
        LabelBase.PREDICATE_COLUMN_NAME,
        LabelBase.OBJECT_COLUMN_NAME
      ).persist(StorageLevel.MEMORY_ONLY)
  }
}
