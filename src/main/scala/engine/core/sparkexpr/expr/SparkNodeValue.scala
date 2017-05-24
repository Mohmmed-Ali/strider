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

  def getResMapping: Any = {
    dataTypeURI match {
      case ExprDataType.boolTypeURI => nv.isBoolean

      case ExprDataType.doubleTypeURI => nv.getDouble

      case ExprDataType.floatTypeURI => nv.getFloat

      case ExprDataType.integerTypeURI => nv.getInteger

      case ExprDataType.stringTypeURI => nv.getString
    }
  }

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkNodeValue {
  def apply(nv: NodeValue): SparkNodeValue = new SparkNodeValue(nv)
}


