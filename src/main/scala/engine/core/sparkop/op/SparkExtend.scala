package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.executor.ExprUDF
import engine.core.sparkexpr.expr.{NullExprException, SparkExpr, VarOutOfBoundException}
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpExtend
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.Expr
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._


/**
  * Created by xiangnanren on 04/09/2017.
  */
class SparkExtend(val opExtend: OpExtend,
                  subOp: SparkOp) extends SparkOp1(subOp: SparkOp) {

  /**
    * The meaning of the attributes in Extend operator:
    *
    * - [[bindingVarTuple]]: (variable, variableName)
    *   The number of binding variables in each Extend operator is supposed to 1.
    *   E.g.,
    *         ( ?o1 - ?o2 ) AS ?measurement, ?measurement is the binding variable
    *         of expression ( ?o1 - ?o2 ).
    *
    * - [[exrpTuple]]:
    *   The expression of the Extend operator and its variable name.
    *   If the expression involves allocated variable, returns null
    *
    * - [[existsAllocVar]]:
    *   Whether the expression contains the allocated/temporary variable or not.
    *   If true, there is no need to re-evaluate the expression tree, i.e., it
    *   just needs to rename the column with temporary name into its binding name.
    *   E.g.,
    *        ?.1 is the allocated/temporary variable,
    *        its binding variable should be ?maxMeasurement
    *
    * - [[funcVarNames]]:
    *   The name of variables in the used function of the expression.
    *   E.g., (?o1 + ?o2), returns Vector(o1, o2)
    *         (?o1 + ?o1), returns Vector(o1, o1)
    *
    * - [[transformedExpr]]:
    *   The transformed Spark-compatible expression from original Jena expression
    *
    * - [[extendUDF]]:
    *    Get udf of the Extend operator.
    */
  private val bindingVarTuple = SparkExtend.getBindVar(opExtend)
  private val exrpTuple = SparkExtend.getExprTuple(opExtend, bindingVarTuple._1)
  private val existsAllocVar= SparkExtend.existAllocVar(exrpTuple._1)
  private val funcVarNames = SparkExtend.getFuncVarNames(exrpTuple._1)
  private val transformedExpr = SparkExtend.transformExpr(exrpTuple._1, this.opName)
  private val extendUDF = SparkExtend.setUDF(existsAllocVar, funcVarNames, transformedExpr)
  
  override def execute(opName: String, child: SparkOpRes): SparkOpRes = {
    if (existsAllocVar) {
      val res = child.result.
        withColumnRenamed(exrpTuple._2, bindingVarTuple._2)
      SparkOpRes(res)
    }
    else {
      val df = child.result
      funcVarNames.length match {
        case 1 =>
          SparkOpRes(df.withColumn(
            bindingVarTuple._2,
            extendUDF.get(col(s"${funcVarNames(0)}"))))
        case 2 => SparkOpRes(df.withColumn(
          bindingVarTuple._2,
          extendUDF.get(
            df(s"${funcVarNames(0)}"),
            df(s"${funcVarNames(1)}"))))
        case 3 => SparkOpRes(df.withColumn(
          bindingVarTuple._2,
          extendUDF.get(
            df(s"${funcVarNames(0)}"),
            df(s"${funcVarNames(1)}"),
            df(s"${funcVarNames(2)}"))))
      }
    }
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }
}

object SparkExtend {
  def apply(opExtend: OpExtend,
            subOp: SparkOp): SparkExtend = new SparkExtend(opExtend, subOp)

  private def getBindVar(opExtend: OpExtend): (Var, String) = {
    opExtend.getVarExprList.
      getVars.toList.length match {
      case l if l > 1 => throw VarOutOfBoundException(
        "The number of binding variable in an Extend" +
          " operator should be not exceeded 1")
      case l if l == 1 =>
        val variable = opExtend.getVarExprList.getVars.toList.head
        (variable, variable.getVarName)
    }
  }

  private def getExprTuple(opExtend: OpExtend,
                           variable: Var): (Expr, String) = {
    val expr = opExtend.getVarExprList.getExprs.get(variable)
    (expr, expr.getVarName)
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

  private def transformExpr(expr: Expr,
                        opName: String): SparkExpr = {
    try {
      (new SparkExprTransformer).
        transform(expr)
    } catch {
      case ex: Exception =>
        throw NullExprException("The expression in" + opName + "is null")
    }
  }

  private def setUDF(existsAllocVar: Boolean,
                     funcVarNames: Vector[String],
                     transformedExpr: SparkExpr): Option[UserDefinedFunction] = {
    funcVarNames.length match {
      case 1 if !existsAllocVar =>
        Option(ExprUDF.doubleTypeUDF_1(funcVarNames, transformedExpr))
      case 2 if !existsAllocVar =>
        Option(ExprUDF.doubleTypeUDF_2(funcVarNames, transformedExpr))
      case 3 if !existsAllocVar =>
        Option(ExprUDF.doubleTypeUDF_3(funcVarNames, transformedExpr))
      case _ => None
    }
  }
}

