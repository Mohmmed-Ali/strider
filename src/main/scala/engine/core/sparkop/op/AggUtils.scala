package engine.core.sparkop.op

import engine.core.sparkexpr.executor.ExprUDF
import engine.core.sparkexpr.expr.SparkExpr
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.functions._

/**
  * Created by xiangnanren on 12/09/2017.
  */
trait AggUtils {
  protected type UDF = UserDefinedFunction
  protected type UADF = UserDefinedAggregateFunction


  protected def generateArithmeticUDF(existsAllocVar: Boolean,
                                      funcVarNames: Vector[String],
                                      transformedExpr: => SparkExpr): Option[UserDefinedFunction] = {
    funcVarNames.length match {
      case 1 if !existsAllocVar =>
        Option(ExprUDF.doubleArithmeticUDF_1(funcVarNames, transformedExpr))
      case 2 if !existsAllocVar =>
        Option(ExprUDF.doubleArithmeticUDF_2(funcVarNames, transformedExpr))
      case 3 if !existsAllocVar =>
        Option(ExprUDF.doubleArithmeticUDF_3(funcVarNames, transformedExpr))
      case _ => None
    }
  }


  protected def evalArithmeticUDF(bindingName: String,
                                  funcArgs: Vector[String],
                                  arithmeticUDF: UserDefinedFunction,
                                  df: DataFrame): DataFrame = {
    funcArgs.length match {
      case 1 => df.withColumn(
        bindingName, arithmeticUDF(col(s"${funcArgs(0)}")))
      case 2 => df.withColumn(
        bindingName, arithmeticUDF(col(s"${funcArgs(0)}"),col(s"${funcArgs(1)}")))
      case 3 => df.withColumn(
        bindingName, arithmeticUDF(col(s"${funcArgs(0)}"),col(s"${funcArgs(1)}"), col(s"${funcArgs(2)}")))
    }
  }

}

