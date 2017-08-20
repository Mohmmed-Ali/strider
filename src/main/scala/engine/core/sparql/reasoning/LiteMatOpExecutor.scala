package engine.core.sparql.reasoning

import engine.core.sparkop.executor.SparkOpExecutor
import engine.core.sparkop.op.SparkBGP
import engine.core.sparkop.op.litematop.LiteMatBGP
import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 20/06/2017.
  */
class LiteMatOpExecutor(inputDF: DataFrame) extends SparkOpExecutor(inputDF){

  override def visit(sparkBGP: SparkBGP): Unit = {
    sparkBGP match {
      case _sparkBGP: LiteMatBGP =>
        stack.push(
          _sparkBGP.execute(_sparkBGP.opName, inputDF)
        )
    }
  }
}

object LiteMatOpExecutor {
  def apply(inputDF: DataFrame): LiteMatOpExecutor = new LiteMatOpExecutor(inputDF)
}