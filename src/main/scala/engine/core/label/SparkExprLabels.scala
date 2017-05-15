package engine.core.label

/**
  * Created by xiangnanren on 03/10/16.
  */
object SparkExprLabels {

  /**
    * The expression labels
    * The right side of equal dedicates the corresponding expression in Spark
    */
  val EXPR_DEFAULT = " EXPR_DEFAULT "

  val EXPR_E_NOT = " NOT "
  val EXPR_E_BOUND = " is not NULL"
  val EXPR_E_NOT_BOUND = " is NULL"

  val EXPR_E_GREATER = " > "
  val EXPR_E_GREATER_OR_EQUAL = " >= "
  val EXPR_E_LESS = " < "
  val EXPR_E_LESS_OR_EQUAL = " <= "
  val EXPR_E_EQUALS = " = "
  val EXPR_E_NOT_EQUALS = " != "
  val EXPR_E_AND = " AND "
  val EXPR_E_OR = " OR "

  val EXPR_E_ADD = " + "
  val EXPR_E_SUBTRACT = " - "
  val EXPR_E_MULTIPLY = " * "

}
