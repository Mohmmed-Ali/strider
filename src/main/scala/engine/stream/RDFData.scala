package engine.stream

import org.apache.jena.rdf.model.Model
import protobuf.Messages

/**
  * Created by xiangnanren on 16/11/2016.
  */
sealed abstract class RDFData extends java.io.Serializable {}

case class RDFTriple(s: String,
                     p: String,
                     o: String) extends RDFData {

  override def toString: String = s + " " + p + " " + o + " ."

}

case class RDFGraph(model: Option[Model] = None,
                    triples: Option[Array[String]] = None) extends RDFData {}

object RDFGraph {
  def apply(model: Model): RDFGraph = new RDFGraph(Some(model))

  def apply(triples: Array[String]): RDFGraph = new RDFGraph(triples = Some(triples))
}

case class WavesEvent(streamId: String,
                      eventId: String,
                      timestamp: Long,
                      model: Model) extends RDFData
