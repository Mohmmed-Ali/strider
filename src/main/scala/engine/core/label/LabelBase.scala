package engine.core.label

/**
  * Created by xiangnanren on 08/07/16.
  */
object LabelBase {
  val INPUT_FILE = "/Users/xiangnanren/IDEAWorkspace/" +
    "spark/rdf/data/chlorine_7TPE_1M.nt"
  val OUTPUT_FILE = "/Users/xiangnanren/" +
    "IDEAWorkspace/spark-rdf-1.0/data/output"
  val INPUT_DATAFRAME_NAME = "STRIDER"
  val KRYO_REGISTRATOR_REF = "engine.core.conf.StriderKryoRegistrator"
  val SUBJECT_COLUMN_NAME = "sDefault"
  val PREDICATE_COLUMN_NAME = "pDefault"
  val OBJECT_COLUMN_NAME = "oDefault"
}