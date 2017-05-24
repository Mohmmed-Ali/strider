package engine.core.sparkexpr.expr

/**
  * Created by xiangnanren on 24/05/2017.
  */
abstract class ExprResMapping extends Serializable

case class BoolMapping(res: Boolean) extends ExprResMapping

case class DoubleMapping(res: Double) extends ExprResMapping

case class FloatMapping(res: Float) extends ExprResMapping

case class IntMapping(res: BigInt) extends ExprResMapping

case class StringMapping(res: String) extends ExprResMapping


object ExprDataType {
  val boolTypeURI: String = "http://www.w3.org/2001/XMLSchema#boolean"
  val doubleTypeURI: String = "http://www.w3.org/2001/XMLSchema#double"
  val floatTypeURI: String = "http://www.w3.org/2001/XMLSchema#float"
  val integerTypeURI: String = "http://www.w3.org/2001/XMLSchema#integer"
  val stringTypeURI: String = "http://www.w3.org/2001/XMLSchema#string"
}