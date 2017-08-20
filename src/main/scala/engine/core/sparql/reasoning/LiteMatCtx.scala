package engine.core.sparql.reasoning

import engine.core.label.LiteMatConfLabels

import scala.collection.mutable

/**
  * Created by xiangnanren on 19/06/2017.
  */

object LiteMatCtx extends LiteMatCtxHelper {
  lazy val settings = new scala.collection.mutable.HashMap[String, String]

  /**
    * Generally, we category two types of dictionaries in Strider.
    *
    *   1) EDCT : for data encoding: | URI/Literal | Encoded_Data |
    *   2) RDCT : for query rewriting: | URI/Literal | Encoded_Data | LowerBound | UpperBound |
    *
    * EDCT is used for data encoding, RDCT is used for query rewriting.
    *
    */
  lazy val EDCT = LiteMatEDCT(
    getEDCTConceptsMapping(
      settings.getOrElse("litemat.dct.concepts", LiteMatConfLabels.CPT_DCT_PATH)),
    getEDCTPropertiesMapping(
      settings.getOrElse("litemat.dct.properties", LiteMatConfLabels.PROP_DCT_PATH)),
    getIndividualsMapping(
      settings.getOrElse("litemat.dct.individuals", LiteMatConfLabels.IND_DCT_PATH))
  )

  lazy val RDCT = LiteMatRDCT(
    getRDCTConceptsMapping(
      settings.getOrElse("litemat.dct.concepts", LiteMatConfLabels.CPT_DCT_PATH)),
    getRDCTPropertiesMapping(
      settings.getOrElse("litemat.dct.properties", LiteMatConfLabels.PROP_DCT_PATH)),
    getIndividualsMapping(
      settings.getOrElse("litemat.dct.individuals", LiteMatConfLabels.IND_DCT_PATH))
  )


}

