package engine.core.sparkop.op

import engine.core.optimizer.conf.AlgebraOptimizerConf
import engine.core.sparkop.compiler.SparkOpVisitor

/**
  * Created by xiangnanren on 07/07/16.
  */
trait SparkOp {
  protected val opSettings = AlgebraOptimizerConf.settings

  def visit(sparkOpVisitor: SparkOpVisitor): Unit
}
