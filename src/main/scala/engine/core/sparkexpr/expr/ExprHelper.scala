package engine.core.sparkexpr.expr

import scala.util.matching.Regex

/**
  * Created by xiangnanren on 24/05/2017.
  */
object ExprHelper {
  val boolTypeURI = "http://www.w3.org/2001/XMLSchema#boolean"
  val decimalTypeURI = "http://www.w3.org/2001/XMLSchema#decimal"
  val doubleTypeURI = "http://www.w3.org/2001/XMLSchema#double"
  val integerTypeURI = "http://www.w3.org/2001/XMLSchema#integer"
  val stringTypeURI = "http://www.w3.org/2001/XMLSchema#string"

  val boolTypeSuffix = "boolean>"
  val decimalTypeSuffix = "decimal>"
  val doubleTypeSuffix = "double>"
  val integerTypeSuffix = "integer>"
  val stringTypeSuffix = "integer>"

  val valueFieldPattern = "[^\"]+".r


  def isQuotedString(arg: String): Boolean = arg.startsWith("\"")

  def getArgValue(arg: String): Any = {
    if (arg.endsWith(ExprHelper.doubleTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get.toDouble
    }
    else if (arg.endsWith(ExprHelper.decimalTypeSuffix)) {
      BigDecimal(valueFieldPattern.findFirstIn(arg).get)
    }
    else if (arg.endsWith(ExprHelper.boolTypeSuffix)) {
      println(valueFieldPattern.findFirstIn(arg).get)
      valueFieldPattern.findFirstIn(arg).get.toBoolean
    }
    else if (arg.endsWith(ExprHelper.integerTypeURI)) {
      BigInt(valueFieldPattern.findFirstIn(arg).get)
    }

  }
}