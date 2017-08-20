package engine.core.sparql.ontology

/**
  * Created by xiangnanren on 01/08/2017.
  */
class IdContainer(val id: Long,
                  val encodingStart: Int,
                  val localLength: Int) {
  override def toString: String =
    "id = " + id +
      ", localLength = " + localLength +
      ", encodingStart =" + encodingStart
}

object IdContainer {
  def apply(id: Long,
            encodingStart: Int,
            localLength: Int): IdContainer =
    new IdContainer(id, encodingStart, localLength)
}