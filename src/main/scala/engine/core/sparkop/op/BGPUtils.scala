package engine.core.sparkop.op

import org.apache.jena.graph
import org.apache.jena.graph.Node
import org.apache.log4j.LogManager
import org.apache.spark.sql._

/**
  * Created by xiangnanren on 16/06/2017.
  */
trait BGPUtils {
  @transient
  protected lazy val log = LogManager.
    getLogger(this.getClass)


  protected def computeTriplePattern(triple: graph.Triple,
                                     inputDF: DataFrame): DataFrame = {
    val tripleS = triple.getSubject
    val tripleP = triple.getPredicate
    val tripleO = triple.getObject
    lazy val normalizedLiteral = tripleO.isLiteral match{
      case true => "\"" + tripleO.getLiteralValue + "\"" +
        "^^<" + tripleO.getLiteralDatatypeURI + ">"
      case false => ""
    }
    lazy val simplifiedLiteral = tripleO.isLiteral match {
      case true => tripleO.getLiteralLexicalForm
      case false => ""
    }

    def normalizeURI(n: Node): String = "<" + n + ">"

    if (tripleS.isVariable && !tripleP.isVariable && !tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleS.getName).
        where(inputDF("pDefault") <=> normalizeURI(tripleP) && inputDF("oDefault") <=> normalizeURI(tripleO)).
        select(tripleS.getName)
    else if (!tripleS.isVariable && tripleP.isVariable && !tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("pDefault", tripleP.getName).
            where(inputDF("sDefault") <=> normalizeURI(tripleS) && (inputDF("oDefault") <=> normalizeURI(tripleO))).
            select(tripleP.getName)
    else if (!tripleS.isVariable && !tripleP.isVariable && tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleO.getName).
            where(inputDF("sDefault") <=> normalizeURI(tripleS) && inputDF("pDefault") <=> normalizeURI(tripleP)).
            select(tripleO.getName)
    else if (tripleS.isVariable && tripleP.isVariable && !tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleS.getName).
            withColumnRenamed("pDefault", tripleP.getName).where(inputDF("oDefault") <=> simplifiedLiteral).
            select(tripleS.getName, tripleP.getName)
    else if (tripleS.isVariable && !tripleP.isVariable && tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleS.getName).
            withColumnRenamed("oDefault", tripleO.getName).where(inputDF("pDefault") <=> normalizeURI(tripleP)).
            select(tripleS.getName, tripleO.getName)
    else if (!tripleS.isVariable && tripleP.isVariable && tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("pDefault", tripleP.getName).
            withColumnRenamed("oDefault", tripleO.getName).where(inputDF("sDefault") <=> normalizeURI(tripleS)).
            select(tripleP.getName, tripleO.getName)
    else if (tripleS.isVariable && tripleP.isVariable && tripleO.isVariable && !tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleS.getName).
            withColumnRenamed("pDefault", tripleP.getName).withColumnRenamed("oDefault", tripleO.getName)
    else if (!tripleS.isVariable && !tripleP.isVariable && !tripleO.isVariable && !tripleO.isLiteral)
      inputDF.where(inputDF("sDefault") <=> normalizeURI(tripleS) &&
            inputDF("pDefault") <=> normalizeURI(tripleP) && inputDF("oDefault") <=> normalizeURI(tripleO))

      // The following cases imply that an object is a literal
    else if (tripleS.isVariable && !tripleP.isVariable && !tripleO.isVariable && tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleS.getName).
        where(inputDF("pDefault") <=> normalizeURI(tripleP) &&
          (inputDF("oDefault") <=> simplifiedLiteral || inputDF("oDefault") <=> normalizedLiteral)).
        select(tripleS.getName)
    else if (!tripleS.isVariable && tripleP.isVariable && !tripleO.isVariable && tripleO.isLiteral)
      inputDF.withColumnRenamed("pDefault", tripleP.getName).
        where(inputDF("sDefault") <=> normalizeURI(tripleS) &&
          (inputDF("oDefault") <=> simplifiedLiteral|| inputDF("oDefault") <=> normalizedLiteral)).
        select(tripleP.getName)
    else if (tripleS.isVariable && tripleP.isVariable && !tripleO.isVariable && tripleO.isLiteral)
      inputDF.withColumnRenamed("sDefault", tripleS.getName).
        withColumnRenamed("pDefault", tripleP.getName).where(inputDF("oDefault") <=> simplifiedLiteral ||
        inputDF("oDefault") <=> normalizedLiteral).select(tripleS.getName, tripleP.getName)
    else if (!tripleS.isVariable && !tripleP.isVariable && !tripleO.isVariable && tripleO.isLiteral)
      inputDF.where(inputDF("sDefault") <=> normalizeURI(tripleS) &&
        inputDF("pDefault") <=> normalizeURI(tripleP) &&
        (inputDF("oDefault") <=> simplifiedLiteral || inputDF("oDefault") <=> normalizedLiteral))

    else throw new UnsupportedOperationException("The input triple pattern is not supported yet.")

  }



//  /**
//    * Compute the projection for each triple pattern in BGP.
//    *
//    * @param triple  : input triple pattern
//    * @param inputDF : input DataFrame
//    * @return : the result DataFrame for current triple pattern.
//    */
//  protected def computeTriplePattern(triple: graph.Triple,
//                                     inputDF: DataFrame): DataFrame = {
//
//    val tStart = System.nanoTime()
//    val tripleS = triple.getSubject
//    val tripleP = triple.getPredicate
//    val tripleO = triple.getObject
//    val normalizedLiteral = tripleO.isLiteral match{
//      case true => "\"" + tripleO.getLiteralValue + "\"" +
//        "^^<" + tripleO.getLiteralDatatypeURI + ">"
//      case false => ""
//    }
//    val simplifiedLiteral = tripleO.isLiteral match {
//      case true => tripleO.getLiteralLexicalForm
//      case false => ""
//    }
//    val tEnd = System.nanoTime()
//    println((tEnd-tStart)/1e6)
//
//    def normalizeURI(n: Node): String = "<" + n + ">"
//
//    val outputDF = (
//      tripleS.isVariable,
//      tripleP.isVariable,
//      tripleO.isVariable,
//      tripleO.isLiteral) match {
//
//      case (true, false, false, false) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
//        where(inputDF("pDefault") <=> normalizeURI(tripleP) && inputDF("oDefault") <=> normalizeURI(tripleO)).
//        select(tripleS.getName)
//
//      case (true, false, false, true) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
//        where(inputDF("pDefault") <=> normalizeURI(tripleP) &&
//          (inputDF("oDefault") <=> simplifiedLiteral || inputDF("oDefault") <=> normalizedLiteral)).
//        select(tripleS.getName)
//
//      case (false, true, false, false) => inputDF.withColumnRenamed("pDefault", tripleP.getName).
//        where(inputDF("sDefault") <=> normalizeURI(tripleS) && (inputDF("oDefault") <=> normalizeURI(tripleO))).
//        select(tripleP.getName)
//
//      case (false, true, false, true) => inputDF.withColumnRenamed("pDefault", tripleP.getName).
//        where(inputDF("sDefault") <=> normalizeURI(tripleS) &&
//          (inputDF("oDefault") <=> simplifiedLiteral|| inputDF("oDefault") <=> normalizedLiteral)).
//        select(tripleP.getName)
//
//      case (false, false, true, false) => inputDF.withColumnRenamed("sDefault", tripleO.getName).
//        where(inputDF("sDefault") <=> normalizeURI(tripleS) && inputDF("pDefault") <=> normalizeURI(tripleP)).
//        select(tripleO.getName)
//
//      case (true, true, false, false) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
//        withColumnRenamed("pDefault", tripleP.getName).where(inputDF("oDefault") <=> simplifiedLiteral).
//        select(tripleS.getName, tripleP.getName)
//
//      case (true, true, false, true) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
//        withColumnRenamed("pDefault", tripleP.getName).
//        where(inputDF("oDefault") <=> simplifiedLiteral || inputDF("oDefault") <=> normalizedLiteral).
//        select(tripleS.getName, tripleP.getName)
//
//      case (true, false, true, false) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
//        withColumnRenamed("oDefault", tripleO.getName).where(inputDF("pDefault") <=> normalizeURI(tripleP)).
//        select(tripleS.getName, tripleO.getName)
//
//      case (false, true, true, false) => inputDF.withColumnRenamed("pDefault", tripleP.getName).
//        withColumnRenamed("oDefault", tripleO.getName).where(inputDF("sDefault") <=> normalizeURI(tripleS)).
//        select(tripleP.getName, tripleO.getName)
//
//      case (true, true, true, false) => inputDF.withColumnRenamed("sDefault", tripleS.getName).
//        withColumnRenamed("pDefault", tripleP.getName).withColumnRenamed("oDefault", tripleO.getName)
//
//      case (false, false, false, false) => inputDF.where(inputDF("sDefault") <=> normalizeURI(tripleS) &&
//        inputDF("pDefault") <=> normalizeURI(tripleP) && inputDF("oDefault") <=> normalizeURI(tripleO))
//
//      case (false, false, false, true) => inputDF.where(inputDF("sDefault") <=> normalizeURI(tripleS) &&
//        inputDF("pDefault") <=> normalizeURI(tripleP) &&
//        (inputDF("oDefault") <=> simplifiedLiteral || inputDF("oDefault") <=> normalizedLiteral))
//
//      case _ => throw new UnsupportedOperationException("The input triple pattern is not supported yet.")
//
//    }
//    outputDF
//  }
}