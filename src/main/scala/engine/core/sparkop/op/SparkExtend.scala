package engine.core.sparkop.op

import engine.core.sparkexpr.expr.ExprElementOutOfBoundException
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpExtend
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 04/09/2017.
  */
class SparkExtend(val opExtend: OpExtend,
                  subOp: SparkOp)
  extends SparkOp1(subOp: SparkOp) with AggUtils{

  /**
    * The meaning of the attributes in Extend operator:
    *
    * - [[bindingVarTuple]]: (variable, variableName)
    * The number of binding variables in each Extend operator is supposed to 1.
    * E.g.,
    * ( ?o1 - ?o2 ) AS ?measurement, ?measurement is the binding variable
    * of expression ( ?o1 - ?o2 ).
    *
    * - [[exrpTuple]]:
    * The expression of the Extend operator and its variable name.
    * If the expression involves allocated variable, returns null
    *
    * - [[existsAllocVar]]:
    * Whether the expression contains the allocated/temporary variable or not.
    * If true, there is no need to re-evaluate the expression tree, i.e., it
    * just needs to rename the column with temporary name into its binding name.
    * E.g.,
    * ?.1 is the allocated/temporary variable,
    * its binding variable should be ?maxMeasurement
    *
    * - [[funcVarNames]]:
    * The name of variables in the used function of the expression.
    * E.g., (?o1 + ?o2), returns Vector(o1, o2)
    * (?o1 + ?o1), returns Vector(o1, o1)
    *
    * - [[transformedExpr]]:
    * The transformed Spark-compatible expression from original Jena expression
    *
    * - [[extendUDF]]:
    * Get udf of the Extend operator.
    */
  private val bindingVarTuple = SparkExtend.getBindVar(opExtend)
  private val exrpTuple = SparkExtend.getExprTuple(opExtend, bindingVarTuple.variable)
  private val existsAllocVar = SparkExtend.existAllocVar(exrpTuple.expr)
  private val funcVarNames = SparkExtend.getFuncVarNames(exrpTuple.expr)
  private val transformedExpr = transformExpr(exrpTuple.expr, this.opName)
  private val extendUDF = generateArithmeticUDF(existsAllocVar, funcVarNames, transformedExpr)

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    if (existsAllocVar) {
      val res = child.result.
        withColumnRenamed(exrpTuple.varName, bindingVarTuple.varName)
      SparkOpRes(res)
    }
    else {
      val res = evalArithmeticUDF(
        bindingVarTuple.varName, funcVarNames,
        extendUDF.get, child.result)

      SparkOpRes(res)
    }
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}

object SparkExtend {
  private case class ExtendExprTuple(expr: Expr, varName: String)
  private case class ExtendBindingVarTuple(variable: Var, varName: String)

  def apply(opExtend: OpExtend,
            subOp: SparkOp): SparkExtend = new SparkExtend(opExtend, subOp)

  private def getBindVar(opExtend: OpExtend): ExtendBindingVarTuple = {
    opExtend.getVarExprList.
      getVars.toList.length match {
      case l if l > 1 => throw ExprElementOutOfBoundException(
        "The number of binding variable in an Extend" +
          " operator should be not exceeded 1")
      case l if l == 1 =>
        val variable = opExtend.getVarExprList.getVars.toList.head
        ExtendBindingVarTuple(variable, variable.getVarName)
    }
  }

  private def getExprTuple(opExtend: OpExtend,
                           variable: Var): ExtendExprTuple = {
    val expr = opExtend.getVarExprList.getExprs.get(variable)
    ExtendExprTuple(expr, expr.getVarName)
  }

  private def existAllocVar(expr: Expr): Boolean = {
    expr.getVarsMentioned match {
      case v if v.size() == 1 && v.iterator().next().isAllocVar => true
      case _ => false
    }
  }

  private def getFuncVarNames(expr: Expr): Vector[String] =
    expr.getFunction match {
      case f if f != null =>
        f.getArgs.collect {
          case _expr if _expr.isVariable => _expr.getVarName
        }.toVector
      case _ => Vector.empty[String]
    }
}



