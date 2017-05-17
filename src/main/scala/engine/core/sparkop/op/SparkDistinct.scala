package engine.core.sparkop.op

import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpDistinct


/**
  * Created by xiangnanren on 13/07/16.
  */

/**
  * The 'distinct' on Spark. Current version simplifies the hierarchy
  * relation from solution modifier and reducedDistinct
  *
  */

class SparkDistinct(val opDistinct: OpDistinct,
                    subOp: SparkOp)
  extends SparkOp1[SparkOp](subOp) {

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    SparkOpRes(child.result.distinct())
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}

object SparkDistinct {
  def apply(opDistinct: OpDistinct,
            subOp: SparkOp): SparkDistinct = new SparkDistinct(opDistinct, subOp)
}