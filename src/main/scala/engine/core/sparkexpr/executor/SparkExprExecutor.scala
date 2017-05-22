package engine.core.sparkexpr.executor

import org.apache.jena.sparql.expr._

/**
  * Created by xiangnanren on 19/05/2017.
  */
class SparkExprExecutor extends ExprVisitor {
  override def visit(func: ExprFunction0): Unit = ???

  override def visit(func: ExprFunction1): Unit = ???

  override def visit(func: ExprFunction2): Unit = ???

  override def visit(func: ExprFunction3): Unit = ???

  override def visit(func: ExprFunctionN): Unit = ???

  override def visit(funcOp: ExprFunctionOp): Unit = ???

  override def visit(nv: NodeValue): Unit = ???

  override def visit(nv: ExprVar): Unit = ???

  override def visit(eAgg: ExprAggregator): Unit = ???

  override def finishVisit(): Unit = ???

  override def startVisit(): Unit = ???
}
