package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.NodeValue

/**
  * Created by xiangnanren on 05/05/2017.
  */
class SparkNodeValue(val nv: NodeValue) extends SparkExpr {


  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkNodeValue {
  def apply(nv: NodeValue): SparkNodeValue = new SparkNodeValue(nv)
}