package engine.core.sparkexpr.expr

import engine.core.sparkexpr.compiler.SparkExprVisitor
import org.apache.jena.sparql.expr.ExprVar

/**
  * Created by xiangnanren on 05/05/2017.
  */
class SparkExprVar(@transient val expr: ExprVar) extends SparkExpr {
  val valueFieldPattern = "\".*?\"".r


  def isQuotedString(arg: String): Boolean = arg.startsWith("\"")

  def getArgValue(arg: String): Any = {
    if (arg.endsWith(ExprHelper.doubleTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get.toDouble
    }
    else if (arg.endsWith(ExprHelper.decimalTypeSuffix)) {
      BigDecimal(valueFieldPattern.findFirstIn(arg).get)
    }
    else if (arg.endsWith(ExprHelper.boolTypeSuffix)) {
      valueFieldPattern.findFirstIn(arg).get.toBoolean
    }
    else if (arg.endsWith(ExprHelper.integerTypeURI)) {
      BigInt(valueFieldPattern.findFirstIn(arg).get)
    }

  }

  def execute(arg: Any) = arg

  override def visit(sparkExprVisitor: SparkExprVisitor): Unit = {
    sparkExprVisitor.visit(this)
  }
}

object SparkExprVar {
  def apply(expr: ExprVar): SparkExprVar = new SparkExprVar(expr)




}