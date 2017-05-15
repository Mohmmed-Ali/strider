package engine.stream

import scala.collection.mutable


/**
  * Created by xiangnanren on 17/11/2016.
  */
class KafkaStreamConsumer(brokerAddr: String,
                          zkConnection: String,
                          groupId: String,
                          params: Option[Map[String, String]],
                          topics: String*
                         ) extends StreamConsumer {

  //  val brokerAddr: String = "localhost:9092"
  //  val topicsSet: Set[String] = Set(MsgRDFTriple.value)
  //  val groupId: String = "consumer-group-test"

  val topicsSet: Set[String] = setTopics().get
  val kafkaParams: Map[String, String] = setDirectConf()

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
    * Get the Kafka and Zookeeper configuration when using
    * receiver based approach
    *
    * @return : Receiver configuration
    */
  def setReceiverConf(): Map[String, String] = Map(
    "zookeeper.connect" -> "localhost:2181",
    "group.id" -> groupId,
    "auto.offset.reset" -> "smallest")

  /**
    * Get the Kafka and Zookeeper configuration when using
    * direct approach (no receiver required), Spark Streaming will
    * track the offset in Kafka by itself
    *
    * @return : Direct stream configuration
    */
  def setDirectConf(): Map[String, String] =
    Map("metadata.broker.list" -> brokerAddr)


  def setTopics[T <: StriderTopic](topics: T*): Option[Set[String]] = {
    try {
      val topicsSet = mutable.Set[String]()
      topics.foreach(topic => topicsSet.add(topic.value))
      Some(topicsSet.toSet)
    } catch {
      case e: Exception => e.printStackTrace(); None;
    }
  }

}


object KafkaStreamConsumer {
  def apply(brokerAddr: String,
            zkConnection: String,
            groupId: String,
            params: Option[Map[String, String]],
            topics: String*
           ): KafkaStreamConsumer = {
    new KafkaStreamConsumer(
      brokerAddr,
      zkConnection,
      groupId,
      params,
      topics: _*)
  }
}