package engine.core.sparql.ontology

import java.io.InputStream
import scala.collection._

/**
  * Created by xiangnanren on 01/08/2017.
  */
class OntologyConverter(EXT: String = "dct",
                        fileName: String,
                        in: InputStream) {
  private val conceptsId2URL = new mutable.HashMap[Long, String]()
  private val conceptsURL2Id = new mutable.HashMap[String, IdContainer]()
  private val propertiesId2URL = new mutable.HashMap[Long, String]()
  private val propertiesURL2Id = new mutable.HashMap[String, IdContainer]()

  def addConceptURL2IdItem(concept: String,
                           id: Long,
                           localLength: Int,
                           encodingStart: Int): this.type = {
    conceptsURL2Id.put(concept, IdContainer(id,encodingStart,localLength))
    this
  }

  def addPropertyURL2IdItem(property: String,
                            id: Long,
                            localLength: Int,
                            encodingStart: Int): this.type = {
    propertiesURL2Id.put(property, IdContainer(id,encodingStart,localLength))
    this
  }

  def displayConcepts(): Unit = for (concept <- conceptsURL2Id.keySet) {
    println(concept + " => " + conceptsURL2Id.get(concept).get.toString)
  }

  def displayProperties(): Unit = for (property <- propertiesURL2Id.keySet) {
    println(property + " => " + propertiesURL2Id.get(property).get.toString)
  }

//  private def getMaxId(itemsURL2Id: Map[String, IdContainer],
//                       idSet: Set[String]): Int = {
//    null
//  }

}















