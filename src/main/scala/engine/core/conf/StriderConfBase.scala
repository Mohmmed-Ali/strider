package engine.core.conf

import com.esotericsoftware.kryo.Kryo
import engine.core.label.LabelBase
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer

/**
  * Created by xiangnanren on 17/11/2016.
  */

/**
  * Strider configuration base initializes the necessary parameters
  * of application. E.g. SparkConf, and kryo encoder/decoder.
  * Strider uses kryo as message encoder and decoder
  * to serialize/deserialize the messages to/from Kafka.
  * The messages of customized data types should be registered in
  * Spark configuration.
  *
  */
sealed abstract class StriderConf {
  val conf: SparkConf
}

class StriderConfBase(concurrentJobs: String = "1",
                      backPressure: String = "false",
                      schedulerMode: String = "FAIR") extends StriderConf {

  val conf = new SparkConf().
    setAppName("Strider").
    setMaster("local[*]").
    set("spark.driver.host", "localhost").
    set("spark.ui.enabled", "true").
    set("spark.ui.showConsoleProgress", "false").
    set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC").
    set("spark.driver.extraJavaOptions", "-XX:+UseConcMarkSweepGC").
    set("spark.locality.wait", "1s").
    set("concurrentJobs", concurrentJobs).
    set("spark.streaming.backpressure.enabled", backPressure).
    set("spark.kryo.registrationRequired", "true").
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrator", LabelBase.KRYO_REGISTRATOR_REF).
    set("spark.scheduler.mode", schedulerMode)
}

object StriderSerializerConf extends StriderConfBase {
  val kryoSerializer = new KryoSerializer(conf)
  val kryo = kryoSerializer.newKryo()

  val kryos: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue(): Kryo = {
      kryoSerializer.newKryo()
    }
  }

}

object StriderDeserializerConf extends StriderConfBase {
  val kryoDeserializer = new KryoSerializer(conf)
  val kryo = new KryoSerializer(conf).newKryo()

  val kryos: ThreadLocal[Kryo] = new ThreadLocal[Kryo]() {
    protected override def initialValue(): Kryo = {
      kryoDeserializer.newKryo()
    }
  }
}

