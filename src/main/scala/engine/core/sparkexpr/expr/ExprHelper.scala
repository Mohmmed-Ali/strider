package engine.core.sparkexpr.expr

import scala.util.Try

/**
  * Created by xiangnanren on 24/05/2017.
  */
private[sparkexpr] object ExprHelper {
  val boolTypeURI = "http://www.w3.org/2001/XMLSchema#boolean"
  val decimalTypeURI = "http://www.w3.org/2001/XMLSchema#decimal"
  val doubleTypeURI = "http://www.w3.org/2001/XMLSchema#double"
  val floatTypeURI = "http://www.w3.org/2001/XMLSchema#float"
  val integerTypeURI = "http://www.w3.org/2001/XMLSchema#integer"
  val stringTypeURI = "http://www.w3.org/2001/XMLSchema#string"

  val boolTypeSuffix = "boolean>"
  val decimalTypeSuffix = "decimal>"
  val doubleTypeSuffix = "double>"
  val floatTypeSuffix = "float>"
  val integerTypeSuffix = "Integer>"
  val stringTypeSuffix = "string>"

  val valueFieldPattern = "[^\"]+".r

  def isQuotedString(arg: String): Boolean = arg.startsWith("\"")

  def getUnquotedString(arg: String): String = {
    if (arg.endsWith(boolTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else if (arg.endsWith(decimalTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else if (arg.endsWith(doubleTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else if (arg.endsWith(floatTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else if (arg.endsWith(integerTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else if (arg.endsWith(stringTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else throw new UnsupportedLiteralException(
      "The input literal is not supported yet " +
        "for the computation of expression. ")
  }

  /**
    * Get the value of attribute in DataFrame.
    * For the performance issue, the BigDecimal and BigInt-typed value
    * are represented by Double and Int, respectively.
    */
  def getArgValue(arg: String): Any = {
    if (arg.endsWith(boolTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get.toBoolean
    }
    else if (arg.endsWith(decimalTypeSuffix)) {
//      BigDecimal(valueFieldPattern.findFirstIn(arg).get)
      valueFieldPattern.findFirstIn(arg).get.toDouble
    }
    else if (arg.endsWith(doubleTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get.toDouble
    }
    else if (arg.endsWith(floatTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get.toFloat
    }
    else if (arg.endsWith(integerTypeSuffix)) {
//      BigInt(valueFieldPattern.findFirstIn(arg).get)
      valueFieldPattern.findFirstIn(arg).get.toInt
    }
    else if (arg.endsWith(stringTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get
    }
    else throw new UnsupportedLiteralException(
      "The input literal is not supported yet " +
        "for the computation of expression. ")
  }


  def getNumValueAsString(arg: String): String = {
    try {
      valueFieldPattern.findFirstIn(arg).get
    } catch {
      case e: Exception => throw UnsupportedLiteralException(
        "The input numeric type is not supported")
    }
  }

  def isNumValue(arg: String): Boolean = {
    if (Try(arg.toDouble).isSuccess) true
    else if (arg.endsWith(decimalTypeSuffix)) true
    else if (arg.endsWith(doubleTypeSuffix)) true
    else if (arg.endsWith(floatTypeSuffix)) true
    else if (arg.endsWith(integerTypeSuffix)) true
    else false
  }
}