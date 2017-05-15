package engine.core.conf.syntaxparser

import scala.util.Try

/**
  * Created by xiangnanren on 21/02/2017.
  */
class ParsedStriderQuery(queryStr: String) extends SyntaxParser {
  val parsedStreamingConfig: Option[Map[String, (Long, String)]] =
    Try(parseAll(getStreamingConfig, queryStr).get).toOption

  val parsedRegisterConfig: Option[List[Map[String, String]]] =
    Try(parseAll(getRegisterConfig, queryStr).get).toOption
}

object ParsedStriderQuery {
  def apply(queryStr: String): ParsedStriderQuery = new ParsedStriderQuery(queryStr)
}
