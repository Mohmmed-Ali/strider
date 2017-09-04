package engine.core.sparkop.op

import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpExtend

/**
  * Created by xiangnanren on 04/09/2017.
  */
class SparkExtend(val opExtend: OpExtend,
                  subOp: SparkOp) extends SparkOp1(subOp: SparkOp) {
  override def execute(opName: String, child: SparkOpRes): SparkOpRes = ???

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = sparkOpVisitor.visit(this)

}

object SparkExtend {
  def apply(opExtend: OpExtend,
            subOp: SparkOp): SparkExtend = new SparkExtend(opExtend, subOp)
}