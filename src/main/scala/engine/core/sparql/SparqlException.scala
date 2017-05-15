package engine.core.sparql

/**
  * Created by xiangnanren on 28/11/2016.
  */
sealed abstract class SparqlException extends Exception

case class InvalidQueryException(msg: String) extends Exception(msg)
