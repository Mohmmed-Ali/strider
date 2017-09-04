package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.NodeValue

/**
  * Created by xiangnanren on 05/05/2017.
  */
class SparkNodeValue(@transient val nv: NodeValue) extends SparkExpr {
  val quotedValue: String = nv.asQuotedString()
  val dataTypeURI: String = nv.getDatatypeURI
  val valueMapping: Any = getValueMapping

  def getValueMapping: Any = {
    dataTypeURI match {
      case ExprHelper.boolTypeURI => nv.isBoolean
      case ExprHelper.decimalTypeURI => nv.getDecimal
      case ExprHelper.doubleTypeURI => nv.getDouble
      case ExprHelper.floatTypeURI => nv.getFloat
      case ExprHelper.integerTypeURI => nv.getInteger
      case ExprHelper.stringTypeURI => nv.getString
    }
  }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkNodeValue {
  def apply(@transient nv: NodeValue): SparkNodeValue = new SparkNodeValue(nv)
}


