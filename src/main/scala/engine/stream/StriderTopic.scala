package engine.stream


/**
  * Created by xiangnanren on 17/11/2016.
  */
sealed trait StriderTopic {
  val key: String
  val value: String
}

/**
  * The data belong to RDFEvent are practically not the topic,
  * the reason why they are tagged as topics is just for preliminary test.
  */
case object MsgRDFTriple extends StriderTopic {
  val key, value = "MsgRDFTriple"
}

case object MsgRDFGraph extends StriderTopic {
  val key, value = "MsgRDFGraph"
}

case object MsgWavesEvent extends StriderTopic {
  val key, value = "MsgWavesEvent"
}

case object MsgNTFile extends StriderTopic {
  val key, value = "MsgNTFile"
}

/**
  * The topics which are listed as below concern the essential
  * meaning of topic, e.g. different types of sensor observation, etc.
  */
case object SensorObservation extends StriderTopic {
  val key, value = "ChlorineObservation"
}
