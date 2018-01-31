package engine.core.sparkop.compiler

import java.io.PrintWriter

import engine.core.sparkop.op._

/**
  * Created by xiangnanren on 11/07/16.
  */
class SparkOpPrinter(logWriter: PrintWriter)
  extends SparkOpVisitorByType {

  private[this] var delimiter: String = ""

  def printAlgebra(op: SparkOp): Unit = {
    op.visit(SparkOpPrinter(logWriter))
  }

  def visit0(op: SparkOp0): Unit = {
    logWriter.println(delimiter + op.opName)
  }

  def visit1(op: SparkOp1[SparkOp]): Unit = {
    logWriter.println(delimiter + op.opName + "(")
    delimiter += "  "
    if (Option(op.subOp).nonEmpty) op.subOp.visit(this)

    delimiter = delimiter.substring(2)
    logWriter.println(delimiter + ")")
  }

  def visit2(op: SparkOp2[SparkOp, SparkOp]): Unit = {
    logWriter.println(delimiter + op.opName + "(")
    delimiter += "  "

    if (Option(op.leftOp).nonEmpty) op.leftOp.visit(this)
    if (Option(op.rightOp).nonEmpty) op.rightOp.visit(this)

    delimiter = delimiter.substring(2)
    logWriter.println(delimiter + ")")
  }

  def visitN(op: SparkOpN): Unit = {
    logWriter.println(delimiter + op.opName + "(")
    delimiter += "  "
  }
}


object SparkOpPrinter {
  def apply(writer: PrintWriter): SparkOpPrinter
  = new SparkOpPrinter(writer)
}