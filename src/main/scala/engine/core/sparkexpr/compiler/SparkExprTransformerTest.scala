package engine.core.sparkexpr.compiler

import engine.core.label.SparkExprLabels
import org.apache.jena.sparql.expr.{ExprAggregator, _}

/**
  * Created by xiangnanren on 06/10/16.
  */
class SparkExprTransformerTest extends ExprVisitor {
  val stack = new scala.collection.mutable.Stack[String]

  /**
    * Transform the filter condition of Sparql to
    * the corresponding filter expression on Spark
    *
    * @param expr The expression in filter condition of Sparql
    * @tparam T The class type with upper bound of Expr
    */
  def transform[T <: Expr](expr: T): String = {
    SparqlExprWalker(this).walkBottomUp(expr)

    val result = stack.isEmpty match {
      case true => ""
      case _ => stack.pop()
    }
    result
  }

  ///////////////////////////////////////////////////////////////////////////
  // Visit method which visits all the expression function
  ///////////////////////////////////////////////////////////////////////////

  def visit[T <: ExprFunction](func: T): Unit =
    func match {
      case f if f.getClass == classOf[ExprFunction0] =>
        visit(f.asInstanceOf[ExprFunction0])

      case f if f.getClass == classOf[ExprFunction1] =>
        visit(func.asInstanceOf[ExprFunction1])

      case f if f.getClass == classOf[ExprFunction2] =>
        visit(func.asInstanceOf[ExprFunction2])

      case f if f.getClass == classOf[ExprFunction3] =>
        visit(func.asInstanceOf[ExprFunction3])

      case f if f.getClass == classOf[ExprFunctionN] =>
        visit(func.asInstanceOf[ExprFunctionN])

      case f if f.getClass == classOf[ExprFunctionOp] =>
        visit(func.asInstanceOf[ExprFunctionOp])
    }


  ///////////////////////////////////////////////////////////////////////////
  // The transformation of operation of ExprFunction on Spark
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Visit an expression of constant, which
    * does not depend on any sub expression.
    */
  override def visit(func: ExprFunction0): Unit = {
    throw new UnsupportedOperationException("ExprFunction0 not supported yet.")
  }

  /**
    * Visit an expression of a single argument.
    */
  override def visit(func: ExprFunction1): Unit = {
    //    val subExpr = stack.pop()

    //    val tempOpExpr = func match {
    //      case f if f.getClass.isInstance(E_LogicalNot) => {
    //        if (func.getArg().isInstanceOf[E_Bound]) ""
    //        else ExprLabels.EXPR_E_NOT
    //      }
    //
    //      case f if f.getClass.isInstance(E_Bound) =>
    //
    //      case f if f.getClass.isInstance(E_Str) || f.getClass.isInstance(E_Str) =>
    //
    //      case _ => ExprLabels.EXPR_DEFAULT
    //    }
    //
    //    val opExpr = tempOpExpr match {
    //      case x if x.equals(ExprLabels.EXPR_DEFAULT) =>
    //        throw new UnsupportedOperationException("ExprFunctionN is not supported.")
    //
    //      case _ =>
    //
    //    }

  }

  /**
    * Visit an expression of two arguments.
    */
  override def visit(func: ExprFunction2): Unit = {

    // Extract the sub-operators of the current expression function 2
    val rightSubExpr: String = stack.pop()
    val leftSubExpr: String = stack.pop()

    /**
      * Concatenation of leftExpr, rightExpr
      */
    val opExpr = func match {
      case f if f.getClass == classOf[E_GreaterThan] =>
        SparkExprLabels.EXPR_E_GREATER

      case f if f.getClass == classOf[E_GreaterThanOrEqual] =>
        SparkExprLabels.EXPR_E_GREATER_OR_EQUAL

      case f if f.getClass == classOf[E_LessThan] =>
        SparkExprLabels.EXPR_E_LESS

      case f if f.getClass == classOf[E_LessThanOrEqual] =>
        SparkExprLabels.EXPR_E_LESS_OR_EQUAL

      case f if f.getClass == classOf[E_Equals] =>
        SparkExprLabels.EXPR_E_EQUALS

      case f if f.getClass == classOf[E_NotEquals] =>
        SparkExprLabels.EXPR_E_NOT_EQUALS

      case f if f.getClass == classOf[E_LogicalAnd] =>
        SparkExprLabels.EXPR_E_AND

      case f if f.getClass == classOf[E_LogicalOr] =>
        SparkExprLabels.EXPR_E_OR

      case f if f.getClass == classOf[E_Add] =>
        SparkExprLabels.EXPR_E_ADD

      case f if f.getClass == classOf[E_Subtract] =>
        SparkExprLabels.EXPR_E_SUBTRACT

      case f if f.getClass == classOf[E_Multiply] =>
        SparkExprLabels.EXPR_E_MULTIPLY

      case _ =>
        throw new UnsupportedOperationException("The given expression is not supported.")
    }
    stack.push("(" +
      leftSubExpr.replace("\"'", "'").replace("'\"", "'") +
      opExpr +
      rightSubExpr.replace("\"'", "'").replace("'\"", "'") +
      ")")
  }

  /**
    * Visit an expression of three arguments.
    */
  override def visit(func: ExprFunction3): Unit = {
    throw new UnsupportedOperationException("ExprFunction3 is not supported.")
  }

  /**
    * Visit an expression of N( or variable, REGEX) arguments.
    */
  override def visit(func: ExprFunctionN): Unit = {
    throw new UnsupportedOperationException("ExprFunctionN is not supported.")
  }

  /**
    * Visit an expression that executes over a pattern.
    */
  override def visit(funcOp: ExprFunctionOp): Unit = {
    throw new UnsupportedOperationException("ExprFunctionOp is not supported.")
  }

  //  override def visit(nv: NodeValue): Unit = {
  //    try {
  //      stack.push("" + Integer.parseInt(
  //        FmtUtils.stringForNode(nv.asNode())
  //      ))
  //    } catch {
  //      case e: NumberFormatException =>
  //        val nodeExpr = FmtUtils.stringForNode(nv.asNode())
  //
  //        val nodeExpr_ = nodeExpr.contains("\"") match {
  //          case true => nodeExpr.replace("\"", "")
  //          case _ =>
  //        }
  //        stack.push("'" + nodeExpr_ + "'")
  //    }
  //  }

  override def visit(nv: NodeValue): Unit = nv.asNode match {
    case node if node.isLiteral => stack.push(node.getLiteralValue.toString)

    case node if node.isURI => stack.push("\"<" + node.getURI + ">\"")
  }

  override def visit(nv: ExprVar): Unit = {
    stack.push(nv.getVarName)
  }

  override def visit(eAgg: ExprAggregator): Unit = {
    throw new UnsupportedOperationException("ExprAggregator is not supported.")
  }

  override def finishVisit(): Unit = {}

  override def startVisit(): Unit = {}

}



