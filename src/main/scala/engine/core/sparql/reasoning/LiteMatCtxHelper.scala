package engine.core.sparql.reasoning

import scala.io.Source

/**
  * Created by xiangnanren on 13/06/2017.
  */
trait LiteMatCtxHelper {
  final val codingPrefix = "E"
  final val conceptPrefix = "C"
  final val propertyPrefix = "P"
  final val individualPrefix = "I"

  /**
    * Get the dictionary of concept for data encoding as Map.
    */
  def getEDCTConceptsMapping(filePath: String): Map[String, String] = {

    try {
      val in = classOf[LiteMatCtxHelper].getResourceAsStream(filePath)
      val iter = Source.fromInputStream(in, "utf-8").getLines().map(_.split(" "))
      (for (i <- iter) yield "<" + i(0) + ">" -> i(1)).toMap
    } catch {
      case e: Exception =>
        throw new LiteMatException("Fail to load input dictionary of concepts")
    }
  }

  /**
    * Get the dictionary of properties for data encoding as Map.
    */
  def getEDCTPropertiesMapping(filePath: String): Map[String, String] = {
    try {
      val in = classOf[LiteMatCtxHelper].getResourceAsStream(filePath)
      val iter = Source.fromInputStream(in, "utf-8").getLines().map(_.split(" "))
      (for (i <- iter) yield "<" + i(0) + ">" -> i(2)).toMap
    } catch {
      case e: Exception =>
        throw new LiteMatException("Fail to load input dictionary of properties")
    }
  }

  /**
    * Get the dictionary of concepts for query rewriting.
    */
  def getRDCTConceptsMapping(filePath: String): Map[String, (String, String)] = {
    try {
      val in = classOf[LiteMatCtxHelper].getResourceAsStream(filePath)
      val iter = Source.fromInputStream(in, "utf-8").getLines().map(_.split(" "))
      (for (i <- iter) yield "<" + i(0) + ">"-> (i(2), i(3))).toMap
    } catch {
      case e: Exception =>
        throw new LiteMatException("Fail to load input dictionary of concepts")
    }
  }

  /**
    * Get the dictionary of properties for query rewriting.
    */
  def getRDCTPropertiesMapping(filePath: String): Map[String, (Int, Int)] = {
    try {
      val in = classOf[LiteMatCtxHelper].getResourceAsStream(filePath)
      val iter = Source.fromInputStream(in, "utf-8").getLines().map(_.split(" "))
      (for (i <- iter)
        yield "<" + i(0) + ">" -> (i(2).toInt, i(3).toInt)).toMap
    } catch {
      case e: Exception =>
        throw new LiteMatException("Fail to load input dictionary of properties")
    }
  }


  /**
    * Get the dictionary of individuals for data encoding.
    */
  def getIndividualsMapping(filePath: String): Map[String, String] = {
    try {
      val in = classOf[LiteMatCtxHelper].getResourceAsStream(filePath)
      val iter = Source.fromInputStream(in, "utf-8").getLines().map(_.split(" "))
      (for (i <- iter) yield "<" + i(0) + ">" -> i(1)).toMap
    } catch {
      case e: Exception => Map.empty[String, String]
    }
  }

}
