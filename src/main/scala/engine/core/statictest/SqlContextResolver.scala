package engine.core.statictest

import engine.core.label.LabelBase
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

/**
  * Created by xiangnanren on 07/07/16.
  */
@Experimental
class SqlContextResolver(sc: SparkContext) {

  val sqlContext = new SQLContext(sc)
  // Setting SQL context configurations for performance tuning
  //  sqlContext.setConf("spark.sql.codegen", "true")
  //  sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
  sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")
  sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "170000000")
  sqlContext.setConf("spark.sql.tungsten.enabled", "true")

  private lazy val rowRDD = SqlContextResolver.createRDDRow(sc)

  val wdf = sqlContext
    .createDataFrame(rowRDD, SqlContextResolver.schema)

  wdf.registerTempTable(LabelBase.INPUT_DATAFRAME_NAME)

  wdf.persist(StorageLevel.MEMORY_ONLY).count()

  val tStart1 = System.nanoTime()
  val resSize = SizeEstimator.estimate(wdf)
  val tEnds1 = System.nanoTime()

}

object SqlContextResolver {

  private val schema = setSchema()

  def apply(sc: SparkContext): SqlContextResolver = new SqlContextResolver(sc)

  private def setSchema(): StructType = {
    val schemaString = s"${LabelBase.SUBJECT_COLUMN_NAME} " +
      s"${LabelBase.PREDICATE_COLUMN_NAME} " +
      s"${LabelBase.OBJECT_COLUMN_NAME}"

    StructType(
      schemaString.split(" ")
        .map(fieldName => StructField(fieldName,
          StringType,
          true: Boolean)))
  }

  private def createRDDRow(sc: SparkContext): RDD[Row] = {
    sc.textFile(
      s"${LabelBase.INPUT_FILE}")
      .map(_.split(" "))
      .map(attribute => Row(attribute(0), attribute(1), attribute(2).trim)).cache()
  }
}