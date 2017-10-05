package engine.stream


/**
  * Created by xiangnanren on 16/11/2016.
  */
sealed abstract class RDFData extends java.io.Serializable {}

case class RDFTriple(s: String,
                     p: String,
                     o: String) extends RDFData {

  override def toString: String = s + " " + p + " " + o + " ."

}

case class RDFGraph(graph: Array[(String, String, String)]) extends RDFData
object RDFGraph {
  def apply(graph: Array[RDFTriple]): RDFGraph = new RDFGraph(graph.map(x => (x.s, x.p, x.o)))
}


