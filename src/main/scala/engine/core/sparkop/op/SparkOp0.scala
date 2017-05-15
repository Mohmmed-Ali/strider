package engine.core.sparkop.op

import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 07/07/16.
  */
abstract class SparkOp0 extends SparkOpBase {
  def execute(opName: String,
              inputDF: DataFrame): SparkOpRes
}