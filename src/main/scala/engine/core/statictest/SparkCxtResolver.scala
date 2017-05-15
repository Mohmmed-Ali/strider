package engine.core.statictest

import engine.core.label.LabelBase
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiangnanren on 07/07/16.
  */
object SparkCxtResolver {
  val conf = new SparkConf().
    setAppName("Strider").
    setMaster("local[*]").
    set("spark.kryo.registrationRequired", "true").
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrator", LabelBase.KRYO_REGISTRATOR_REF)
  val sc = new SparkContext(conf)
}
