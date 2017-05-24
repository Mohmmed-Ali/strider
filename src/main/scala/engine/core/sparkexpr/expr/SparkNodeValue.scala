package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.NodeValue

/**
  * Created by xiangnanren on 05/05/2017.
  */
class SparkNodeValue(@transient val nv: NodeValue) extends SparkExpr {
  val quotedValue = nv.asQuotedString()
  val dataTypeURI = nv.getDatatypeURI
  val resMapping = getResMapping

  def getResMapping: ExprResMapping = {
    dataTypeURI match {
      case ExprDataType.boolTypeURI => BoolMapping(nv.isBoolean)

      case ExprDataType.doubleTypeURI => DoubleMapping(nv.getDouble)

      case ExprDataType.floatTypeURI => FloatMapping(nv.getFloat)

      case ExprDataType.integerTypeURI => IntMapping(nv.getInteger)

      case ExprDataType.stringTypeURI => StringMapping(nv.getString)
    }
  }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkNodeValue {
  def apply(nv: NodeValue): SparkNodeValue = new SparkNodeValue(nv)
}


