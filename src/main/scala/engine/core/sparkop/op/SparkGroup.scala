package engine.core.sparkop.op

import engine.core.sparkop.compiler.SparkOpVisitor
import engine.core.sparkop.op.SparkGroup.{ExprTemplate, UDFParameters, UDFTuple}
import org.apache.jena.sparql.algebra.op.OpGroup
import org.apache.jena.sparql.expr.aggregate._
import org.apache.jena.sparql.expr.ExprFunction
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


/**
  * Created by xiangnanren on 02/08/16.
  */
class SparkGroup(val opGroup: OpGroup,
                 subOp: SparkOp)
  extends SparkOp1(subOp: SparkOp) with AggUtils {

  private val groupVars = setGroupVarName()
  private val nameMapping = createNameMapping()
  private val arithmeticExprTemplates = setExprTemplates()
  private val aggUDFs = generateAggUDFs()

  override def execute(opName: String,
                       child: SparkOpRes): SparkOpRes = {
    val resArithmetic = evalArithmeticExprs(
      arithmeticExprTemplates, child.result)
    SparkOpRes(executeAggExprs(resArithmetic))
  }

  override def visit(sqlOpVisitor: SparkOpVisitor): Unit = {
    sqlOpVisitor.visit(this)
  }

  private def setGroupVarName(): Vector[String] = {
    opGroup.getGroupVars.getVars.map(x => x.getVarName).toVector
  }

  /**
    * Create the mapping from temporary binding column name
    * to original binding column name.
    *
    * i.e., temporaryBingName = prefix + originalBindingName
    *
    * The reason to create temporary binding name is to buffer the
    * intermediate result of an aggregation which contains arithmetic
    * expression.
    *
    * E.g., for the given expression of aggregation min(?o1 + ?o2),
    *
    *     originalBindingName = .0 (assigned by Jena ARQ);
    *     temporaryBindingName = TEMP.0,
    *
    *   TEMP.0 is used for saving the result of ?o1 + ?o2.
    *
    * The TEMP.0 will be the column to perform min aggregation,
    * and finally the system renames TEMP.0 with .0 .
    *
    * @return (newBindingName, originalBindingName)
    */
  private def createNameMapping(): Map[String, String] = {
    val mapping = opGroup.getAggregators.map { agg =>
      val aggExpr = agg.getAggregator.getExprList.getList.head
      val originalBindingName = agg.getAggVar.getVarName

      val newBindingName = Option(aggExpr.getFunction) match {
        case f if f.isEmpty => originalBindingName
        case f if f.nonEmpty =>
          SparkGroup.tempNamePrefix + originalBindingName
      }
      newBindingName -> originalBindingName
    }.toMap

    mapping
  }

  /**
    * @return List(newBindingName, (expr, funcVarNames))
    */
  private def setExprTemplates(): List[ExprTemplate] = {
    val getArgs = (f: ExprFunction) => {
      f.getArgs.collect {
        case _expr if _expr.isVariable => _expr.getVarName
      }.toVector
    }

    val templates = opGroup.getAggregators.map { agg =>
      val aggExpr = agg.getAggregator.getExprList.getList.head
      val transformedExpr = transformExpr(aggExpr, this.opName)
      val originalBindingName = agg.getAggVar.getVarName
      val aggUDF = agg.getAggregator match {
        case a : AggMax => AggMaxWrapper
        case a : AggMin => AggMinWrapper
        case a : AggAvg => AggAvgWrapper
        case a : AggSum => AggSumWrapper
        case a : AggCount => AggCountWrapper
      }

      Option(aggExpr.getFunction) match {
        case Some(f) =>
          val funcArgs = getArgs(f)
          val newBindingName = SparkGroup.tempNamePrefix + originalBindingName
          val arithmeticUDF = generateArithmeticUDF(
            false: Boolean, funcArgs, transformedExpr)
          ExprTemplate(
            UDFParameters(newBindingName, funcArgs),
            UDFTuple(arithmeticUDF, aggUDF.aggregator))

        case None =>
          val varName = aggExpr.getVarsMentioned.head.getVarName
          val arithmeticUDF = generateArithmeticUDF(
            true: Boolean, Vector(varName), transformedExpr)
          ExprTemplate(
            UDFParameters(originalBindingName, Vector(varName)),
            UDFTuple(arithmeticUDF, aggUDF.aggregator))
      }
    }.toList

    templates
  }

  /**
    * When the column name is created by col("name"),
    * it must include backtick, i.e. col("`name`")
    *
    * @return
    */
  private def generateAggUDFs(): Seq[Column] = {
    val aggUDFs = new ListBuffer[Column]()

    arithmeticExprTemplates.foreach { template =>
      template.udfTuple match {
        case udfTuple if udfTuple.arithmeticUDF.nonEmpty =>
          val tempBindingName = template.parameters.bindingName
          val originalBindingName = nameMapping(tempBindingName)
          val aggUDF = udfTuple.aggUDF(col(s"`$tempBindingName`")).
            as(s"$originalBindingName")
          aggUDFs.append(aggUDF)

        case  udfTuple if udfTuple.arithmeticUDF.isEmpty =>
          val tempBindingName = template.parameters.args.head
          val originalBindingName =  template.parameters.bindingName
          val aggUDF = udfTuple.aggUDF(col(s"`$tempBindingName`")).
            as(s"$originalBindingName")
          aggUDFs.append(aggUDF)
      }
    }
    aggUDFs
  }


  /**
    * The general steps for the evaluation of the expression:
    *     1. Evaluate all possible arithmetic expression;
    *     2. Evaluate the groupBy operator;
    *     3. Evaluate the aggregate expression
    */
  private def evalSingleArithmeticExpr(template: ExprTemplate,
                                       df: DataFrame): DataFrame = {
    template.udfTuple.arithmeticUDF match {
      case Some(arith) =>
        val tempBindingName = template.parameters.bindingName
        evalArithmeticUDF(tempBindingName,
          template.parameters.args, arith, df)
        case None => df
      }
  }

  /**
    * If current group operator contains multiple
    * arithmetic expression, it should be evaluated recursively.
    *
    * E.g., df0 is the input DataFrame, df2 is supposed to be the output.
    * It exists two arithmetic expressions expr1, expr2 should be evaluated:
    *
    *    df1 = eval(df0, expr1)
    *    df2 = eval(df1) = eval(eval(df0, expr1), expr2)
    */
  private def evalArithmeticExprs(initExprTemplates: List[ExprTemplate],
                                  inputDF: DataFrame): DataFrame = {
    @tailrec
    def evalExprAccumulator(exprTemplates: List[ExprTemplate],
                            accumDF: DataFrame): DataFrame = {
      exprTemplates match {
        case Nil => accumDF
        case e :: tail => evalExprAccumulator(
          tail, evalSingleArithmeticExpr(e, accumDF))
      }
    }
    evalExprAccumulator(initExprTemplates, inputDF)
  }


  /**
    * The method corresponds to step 2 and 3
    *
    * @return
    */
  private def executeAggExprs (inputDF: DataFrame):
  DataFrame = {
    (groupVars.length, aggUDFs.length) match {
      case (g_l, a_l) if g_l == 1 && a_l == 1 =>
        inputDF.groupBy(groupVars.head).
          agg(aggUDFs.head)
      case (g_l, a_l) if g_l == 1 && a_l > 1 =>
        inputDF.groupBy(groupVars.head).
          agg(aggUDFs.head, aggUDFs.tail: _*)
      case (g_l, a_l) if g_l > 1 && a_l == 1 =>
        inputDF.groupBy(groupVars.head, groupVars.tail: _*).
          agg(aggUDFs.head)
      case (g_l, a_l) if g_l > 1 && a_l > 1 =>
        inputDF.groupBy(groupVars.head, groupVars.tail: _*).
          agg(aggUDFs.head, aggUDFs.tail: _*)
      case (g_l, a_l) if g_l == 0 && a_l == 1 =>
        inputDF.agg(aggUDFs.head)
      case (g_l, a_l) if g_l == 0 && a_l > 1 =>
        inputDF.agg(aggUDFs.head, aggUDFs.tail: _*)
    }
  }
}


object SparkGroup {
  private case class ExprTemplate(parameters: UDFParameters, udfTuple: UDFTuple)
  private case class UDFTuple(arithmeticUDF: Option[UserDefinedFunction],
                              aggUDF: UserDefinedAggregateFunction)
  private case class UDFParameters(bindingName: String, args: Vector[String])
  private val tempNamePrefix = "TEMP"

  def apply(opGroup: OpGroup,
            subOp: SparkOp): SparkGroup = new SparkGroup(opGroup, subOp)

}