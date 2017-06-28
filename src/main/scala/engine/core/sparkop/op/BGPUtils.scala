package engine.core.sparkop.op

import org.apache.jena.graph
import org.apache.log4j.LogManager
import org.apache.spark.sql._

/**
  * Created by xiangnanren on 16/06/2017.
  */
trait BGPUtils {
  @transient
  protected lazy val log = LogManager.
    getLogger(this.getClass)

  /**
    * Compute the projection for each triple pattern in BGP.
    *
    * @param triple  : input triple pattern
    * @param inputDF : input DataFrame
    * @return : the result DataFrame for current triple pattern.
    */
  protected def computeTriplePattern(triple: graph.Triple,
                                     inputDF: DataFrame): DataFrame = {
    val tripleS = triple.getSubject
    val tripleP = triple.getPredicate
    val tripleO = triple.getObject

    val outputDF = (
      tripleS.isVariable,
      tripleP.isVariable,
      tripleO.isVariable
      ) match {
      case (true, false, false) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
        where(inputDF("pDefault") <=> "<" + tripleP + ">").
        where(inputDF("oDefault") <=> "<" + tripleO + ">").
        select(tripleS.getName)
      case (false, true, false) => inputDF.withColumnRenamed("pDefault", tripleP.getName).
        where(inputDF("sDefault") <=> "<" + tripleS + ">").
        where(inputDF("oDefault") <=> "<" + tripleO + ">").
        select(tripleP.getName)
      case (false, false, true) => inputDF.withColumnRenamed("sDefault", tripleO.getName).
        where(inputDF("sDefault") <=> "<" + tripleS + ">").
        where(inputDF("pDefault") <=> "<" + tripleP + ">").
        select(tripleO.getName)
      case (true, true, false) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
        withColumnRenamed("pDefault", tripleP.getName).
        where(inputDF("oDefault") <=> "<" + tripleO + ">").
        select(tripleS.getName, tripleP.getName)
      case (true, false, true) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
        withColumnRenamed("oDefault", tripleO.getName).
        where(inputDF("pDefault") <=> "<" + tripleP + ">").
        select(tripleS.getName, tripleO.getName)
      case (false, true, true) => inputDF.withColumnRenamed("pDefault", tripleP.getName).
        withColumnRenamed("oDefault", tripleO.getName).
        where(inputDF("sDefault") <=> "<" + tripleS + ">").
        select(tripleP.getName, tripleO.getName)
      case (true, true, true) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
        withColumnRenamed("pDefault", tripleP.getName).
        withColumnRenamed("oDefault", tripleO.getName)
      case (false, false, false) => inputDF.where(inputDF("sDefault") <=> "<" + tripleS + ">").
        where(inputDF("pDefault") <=> "<" + tripleP + ">").
        where(inputDF("oDefault") <=> "<" + tripleO + ">")
    }
    outputDF
  }

}
