package engine.core.label

/**
  * Created by xiangnanren on 08/07/16.
  */
object LabelBase {
//  val INPUT_FILE = "/Users/xiangnanren/IDEAWorkspace/" +
//    "spark/rdf/data/chlorine_7TPE_1M.nt"

//  val INPUT_FILE = "/Users/xiangnanren/IDEAWorkspace/" +
//    "strider/data/lubm_test_data/univ_lubm2.nt" // Test Data for LiteMat

  val INPUT_FILE = "/Users/xiangnanren/IDEAWorkspace/datastores/lubm/univ_lubm100.nt"

  val OUTPUT_FILE = "/Users/xiangnanren/" +
    "IDEAWorkspace/spark-rdf-1.0/data/output"
  val INPUT_DATAFRAME_NAME = "STRIDER"
  val KRYO_REGISTRATOR_REF = "engine.core.conf.StriderKryoRegistrator"
  val SUBJECT_COLUMN_NAME = "sDefault"
  val PREDICATE_COLUMN_NAME = "pDefault"
  val OBJECT_COLUMN_NAME = "oDefault"
}