package engine.core.sparkop.op.litematop

import engine.core.sparkop.op.{SparkBGP, SparkOpRes}
import org.apache.jena.graph
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.spark.sql.DataFrame

/**
  * Created by xiangnanren on 19/06/2017.
  */
class LiteMatBGP(opBGP: OpBGP,
                 triples: List[graph.Triple])
  extends SparkBGP(opBGP, triples) with LiteMatBGPUtils{
  private val liteMatEPHandler = LiteMatEPHandler(ucg.ucgNodes)

  def computeLiteMatBGP(inputDF: DataFrame): DataFrame = {
    val dfMap = computeDFMapViaLiteMat(
      liteMatEPHandler.liteMatBGPNodeMapping, inputDF)

    computeBGP(dfMap, inputDF)
  }

  override def execute(opName: String,
                       inputDF: DataFrame): SparkOpRes = {
    SparkOpRes(computeLiteMatBGP(inputDF))
  }

}

object LiteMatBGP {
  def apply(opBGP: OpBGP,
            triples: List[Triple]): LiteMatBGP = new LiteMatBGP(opBGP, triples)
}