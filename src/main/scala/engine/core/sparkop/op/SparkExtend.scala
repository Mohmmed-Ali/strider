package engine.core.sparkop.op

import engine.core.sparkexpr.compiler.SparkExprTransformer
import engine.core.sparkexpr.executor.ExprUDF
import engine.core.sparkexpr.expr.{NullExprException, VarOutOfBoundException}
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.sparql.algebra.op.OpExtend
import org.apache.spark.sql.DataFrame
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
    * - [[bindingVar]]:
    *   The number of binding variables in each Extend operator is supposed to 1.
    *   E.g.,
    *         ( ?o1 - ?o2 ) AS ?measurement, ?measurement is the binding variable
    *         of expression ( ?o1 - ?o2 ) }}}
    *
    * - [[bindingVarName]]:
    *   bindingVarName refers to the name of bindingVar, e.g.,
    *   ?measurement: bindingVar => measurement: bindingVarName
    *
    * - [[expr]]:
    *   The expression of Extend operator
    *
    * - [[existsAllocVar]]:
    *   Whether the expression contains the allocated/temporary variable or not.
    *   If true, there is no need to re-evaluate the expression tree, i.e., it
    *   just needs to rename the column with temporary name into its binding name.
    *   E.g.,
    *        ?.1 is the allocated/temporary variable,
    *        its binding variable should be ?maxMeasurement
    *
    * - [[exprVarNames]]:
    *   The name of variables in a given expression
    *
    * - [[transformedExpr]]:
    *   The transformed Spark-compatible expression from original Jena expression
    *
    * - [[]]
    */
  private val bindingVar = opExtend.getVarExprList.
    getVars.toList.length match {
    case l if l > 1 => throw VarOutOfBoundException(
      "The number of binding variable in an Extend" +
        " operator should be not exceeded 1")
    case l if l == 1 =>  opExtend.getVarExprList.getVars.toList.head
  }
  private val bindingVarName = bindingVar.getVarName
  private val expr = opExtend.getVarExprList.getExprs.get(bindingVar)
  private val existsAllocVar= expr.getVarsMentioned match {
    case v if v.size() == 1 && v.iterator().next().isAllocVar => true
    case _ => false
  }

  private val exprVarNames = expr.getFunction.getArgs.
    collect {
      case _expr if _expr.isVariable => _expr.getVarName
    }.toVector

  private val transformedExpr = try {
    (new SparkExprTransformer).transform(expr)
  } catch {
    case ex: Exception =>
      throw NullExprException("The expression in" + this.opName + "is null")
  }
  private val extendUDF = exprVarNames.length match {
    case 1 if !existsAllocVar =>
      Option(ExprUDF.doubleTypeUDF_1(exprVarNames, transformedExpr))
    case 2 if !existsAllocVar =>
      Option(ExprUDF.doubleTypeUDF_2(exprVarNames, transformedExpr))
    case 3 if !existsAllocVar =>
      Option(ExprUDF.doubleTypeUDF_3(exprVarNames, transformedExpr))
    case _ => None
  }
  private val extendUDF_1 = ExprUDF.doubleTypeUDF_1(exprVarNames, transformedExpr)


  override def execute(opName: String, child: SparkOpRes): SparkOpRes = {
    if (existsAllocVar) {
      val res = child.result.
        withColumnRenamed(exprVarNames.head, bindingVarName)
      SparkOpRes(res)
    }
    else {
      val df = child.result
      exprVarNames.length match {
        case 1 =>
          SparkOpRes(df.withColumn(bindingVarName, extendUDF_1(col(s"${exprVarNames(0)}"))))
        case 2 => SparkOpRes(df.withColumn(
          bindingVarName,
          extendUDF.get(
            df(s"${exprVarNames(0)}"),
            df(s"${exprVarNames(1)}"))))
        case 3 => SparkOpRes(df.withColumn(
          bindingVarName,
          extendUDF.get(
            df(s"${exprVarNames(0)}"),
            df(s"${exprVarNames(1)}"),
            df(s"${exprVarNames(2)}"))))
      }
    }
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = sparkOpVisitor.visit(this)




}

object SparkExtend {
  def apply(opExtend: OpExtend,
            subOp: SparkOp): SparkExtend = new SparkExtend(opExtend, subOp)
}

