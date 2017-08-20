package engine.core.sparql.reasoning

/**
  * Created by xiangnanren on 13/06/2017.
  */
sealed abstract class Dictionary extends Serializable

case class LiteMatEDCT(conceptMapping: Map[String, String],
                       propertyMapping: Map[String, String],
                       individualMapping: Map[String, String]) extends Dictionary

object LiteMatEDCT {
  def apply(conceptMapping: Map[String, String],
            propertyMapping: Map[String, String]): LiteMatEDCT =
    new LiteMatEDCT(conceptMapping, propertyMapping, Map.empty[String,String])
}


case class LiteMatRDCT(conceptMapping: Map[String, (String, String)],
                       propertyMapping: Map[String, (Int, Int)],
                       individualMapping: Map[String, String]) extends Dictionary

object LiteMatRDCT {
  def apply(conceptMapping: Map[String, (String, String)],
            propertyMapping: Map[String, (Int, Int)]): LiteMatRDCT =
    new LiteMatRDCT(conceptMapping, propertyMapping, Map.empty[String,String])
}