package engine.stream

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringEncoder
import org.apache.log4j.LogManager

import scala.collection.mutable

/**
  * Created by xiangnanren on 16/11/2016.
  */
class KafkaStreamProducer(brokerAddr: String,
                          zkConnection: String,
                          groupId: String,
                          params: Option[Map[String, String]],
                          topics: String*
                         ) extends StreamProducer
  with Serializable {
  @transient
  lazy val log = LogManager.getLogger(this.getClass)

  val topicsSet: Set[String] = setTopics().get
  val kafkaParamProps: Properties = getConfigProps

  /**
    * Get Kafka configuration in properties format
    */
  def getConfigProps: Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddr)
    props.put("key.serializer.class", classOf[StringEncoder].getName)
    props.put("serializer.class", classOf[StreamSerializer[RDFTriple]].getName)

    props
  }

  /**
    * Get Kafka configuration in Map format
    */
  def getConfigMaps: Map[String, String] =
    Map("metadata.broker.list" -> brokerAddr,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "engine.stream.StreamSerializer")


  def setTopics(): Option[Set[String]] = {
    try {
      val topicsSet = mutable.Set[String]()
      topics.foreach(topic => topicsSet.add(topic))
      Some(topicsSet.toSet)
    } catch {
      case e: Exception => e.printStackTrace(); None;
    }
  }

  /**
    * Send different types of messages by Kafka producer.
    * Note that Kafka achieves the best throughput when the message
    * size is around 10k (cf official docs)
    *
    * @param producer : Kafka message producer
    * @param message  : Different types of messages (< RDFEvent)
    */
  def sendMessage[T <: RDFData](producer: Producer[String, T],
                                message: T): Unit = {

    message match {

      case _message: RDFTriple =>
        producer.asInstanceOf[Producer[String, RDFTriple]].
          send(
            new KeyedMessage[String, RDFTriple](
              MsgRDFTriple.key,
              MsgRDFTriple.value,
              _message
            ))

      case _message: RDFGraph =>
        producer.asInstanceOf[Producer[String, RDFGraph]].
          send(
            new KeyedMessage[String, RDFGraph](
              MsgRDFGraph.key,
              MsgRDFGraph.value,
              _message
            ))

      case _message: WavesEvent =>
        producer.asInstanceOf[Producer[String, WavesEvent]].
          send(
            new KeyedMessage[String, WavesEvent](
              MsgWavesEvent.key,
              MsgWavesEvent.value,
              _message
            ))

    }
  }
}

object KafkaStreamProducer {
  def apply(brokerAddr: String,
            zkQuorum: String,
            groupId: String,
            params: Option[Map[String, String]],
            topics: String*): KafkaStreamProducer = {
    new KafkaStreamProducer(
      brokerAddr,
      zkQuorum,
      groupId,
      params,
      topics: _*
    )
  }

}