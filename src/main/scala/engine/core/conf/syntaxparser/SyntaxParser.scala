package engine.core.conf.syntaxparser

import org.apache.log4j.LogManager

import scala.util.parsing.combinator.JavaTokenParsers

/**
  * Created by xiangnanren on 21/02/2017.
  */
trait SyntaxParser extends JavaTokenParsers {
  @transient
  protected lazy val log = LogManager.
    getLogger(this.getClass)
  protected val STREAMING = SyntaxToken("STREAMING")
  protected val WINDOW = SyntaxToken("WINDOW")
  protected val SLIDE = SyntaxToken("SLIDE")
  protected val BATCH = SyntaxToken("BATCH")
  protected val MILLISECONDS = SyntaxToken("MILLISECONDS")
  protected val SECONDS = SyntaxToken("SECONDS")
  protected val MINUTES = SyntaxToken("MINUTES")
  protected val REGISTER = SyntaxToken("REGISTER")
  protected val QUERYID = SyntaxToken("QUERYID")
  protected val SPARQL = SyntaxToken("SPARQL")
  protected val ANYCHAR = SyntaxToken("[^\\[^\\]]*")

  /**
    * Get the streaming context configuration.
    *
    * @return The map of streaming context configurations (keyword -> config).
    */
  protected def getStreamingConfig: Parser[Map[String, (Long, String)]] =
    streamingClause ~ rep(registerClause) ^^ {
      case s ~ r => s
    }

  /**
    * Get the configuration of queries.
    *
    * @return The list of query context configurations.
    */
  protected def getRegisterConfig: Parser[List[Map[String, String]]] =
    streamingClause ~ rep(registerClause) ^^ {
      case s ~ r => r
    }

  /**
    * Parse the clause of streaming context.
    * WINDOW: The range of windowing operator;
    * SLIDE: The sliding step of windowing operator;
    * BATCH: The batch interval of DStream.
    *
    * @return Map[]
    */
  private[this] def streamingClause: Parser[Map[String, (Long, String)]] =
    STREAMING.pattern ~! "{" ~> initS2ROp <~ "}"

  private[this] def initS2ROp: Parser[Map[String, (Long, String)]] = S2ROpOrder ^^ {
    case op1 ~ op2 ~ op3 =>
      Seq(op1, op2, op3).map(op => op._1 -> op._2).toMap
  }

  private[this] def S2ROpOrder =
    (window ~ slide ~ batch) |
      (window ~ batch ~ slide) |
      (slide ~ window ~ batch) |
      (batch ~ window ~ slide) |
      (batch ~ slide ~ window) |
      (slide ~ batch ~ window)

  private[this] def streamingArgs: Parser[(Long, String)] =
    num ~ durationType ^^ {
      case n ~ d => (n, d)
    }

  private[this] def durationType: Parser[String] =
    milliseconds | seconds | minutes

  private[this] def milliseconds: Parser[String] =
    MILLISECONDS.pattern ^^ { d => MILLISECONDS.normalized }

  private[this] def seconds: Parser[String] =
    SECONDS.pattern ^^ { d => SECONDS.normalized }

  private[this] def minutes: Parser[String] =
    MINUTES.pattern ^^ { d => MINUTES.normalized }

  private[this] def num: Parser[Long] =
    floatingPointNumber ^^ {
      _.toLong
    }

  private[this] def window: Parser[(String, (Long, String))] =
    WINDOW.pattern ~! "[" ~> streamingArgs <~ "]" ^^ {
      args => (WINDOW.normalized, args)
    }

  private[this] def slide: Parser[(String, (Long, String))] =
    SLIDE.pattern ~! "[" ~> streamingArgs <~ "]" ^^ {
      args => (SLIDE.normalized, args)
    }

  private[this] def batch: Parser[(String, (Long, String))] =
    BATCH.pattern ~! "[" ~> streamingArgs <~ "]" ^^ {
      args => (BATCH.normalized, args)
    }

  /**
    * Parse the clauses of SPARQL context to register multi queries
    *
    * E.g.: List(query 1, query 2)
    * query_1: Map(QUERYID -> query_Id_1, SPARQL -> queryStr_1)
    * query_2: Map(QUERYID -> query_Id_2, SPARQL -> queryStr_2)
    *
    * @return A list of multi parsed queries.
    */
  private[this] def registerClause: Parser[Map[String, String]] =
    REGISTER.pattern ~ "{" ~> rep(registerOp) <~ "}" ^^ {
      case op => Map() ++ op
    }

  private[this] def registerOp: Parser[(String, String)] =
    queryId | sparql

  private[this] def queryId: Parser[(String, String)] =
    QUERYID.pattern ~ "[".? ~> ANYCHAR.pattern <~ "]" ^^ {
      v => (QUERYID.normalized, v)
    }

  private[this] def sparql: Parser[(String, String)] =
    SPARQL.pattern ~ "[".? ~> ANYCHAR.pattern <~ "]" ^^ {
      v => (SPARQL.normalized, v)
    }

  /**
    * Define the keywords.
    *
    * @param keyword : The keywords defined in syntax. 'pattern' presents the regex pattern,
    *                'normalized' converts input keywords into uppercase
    */
  protected class SyntaxToken(keyword: String) {
    val pattern = s"(?i)$keyword".r
    val normalized = keyword.toUpperCase
  }


  protected object SyntaxToken {
    def apply(keyword: String): SyntaxToken = new SyntaxToken(keyword)
  }
}



