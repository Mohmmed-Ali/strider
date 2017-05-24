package engine.core.sparkexpr.compiler

import org.apache.jena.sparql.expr.{ExprAggregator, ExprVar, NodeValue, _}

/**
  * Created by xiangnanren on 06/10/16.
  */
class OriginalExprWalker(private val exprVisitor: ExprVisitor)
  extends ExprVisitorFunction {

  def walkBottomUp(expr: Expr) = {
    expr.visit(OriginalExprWalker(exprVisitor))
  }

  /**
    * Override the method in superclass, invoked by
    * visit(func: ExprFunction1), visit(func: ExprFunction2)...
    */
  override def visitExprFunction(func: ExprFunction): Unit = {
    (1 to func.numArgs()).
      takeWhile(i => Option(func.getArg(i)).nonEmpty).
      foreach(i => func.getArg(i).visit(this))

    func.visit(exprVisitor)
  }

  override def visit(funcOp: ExprFunctionOp): Unit = {
    funcOp.visit(exprVisitor)
  }

  override def visit(nv: NodeValue): Unit = {
    nv.visit(exprVisitor)
  }

  override def visit(nv: ExprVar): Unit = {
    nv.visit(exprVisitor)
  }

  override def visit(eAgg: ExprAggregator): Unit = {
    eAgg.visit(exprVisitor)
  }

  override def finishVisit(): Unit = {}

  override def startVisit(): Unit = {}

}


object OriginalExprWalker {
  def apply(exprVisitor: ExprVisitor): OriginalExprWalker =
    new OriginalExprWalker(exprVisitor)
}
