package engine.stream


/**
  * Created by xiangnanren on 17/11/2016.
  */
sealed trait StriderTopic {
  val defaultTopic: String
  def distributeKey(partitionsNum: => Int): String = {
    scala.util.Random.nextInt( partitionsNum + 1 ).toString
  }
}

/**
  * The data belong to RDFEvent are practically not the topic,
  * the reason why they are tagged as topics is just for preliminary test.
  */
case object MsgRDFTriple extends StriderTopic {
  val defaultTopic = "MsgRDFTriple"
}

case object MsgRDFGraph extends StriderTopic {
  val defaultTopic = "MsgRDFGraph"
}

case object MsgWavesEvent extends StriderTopic {
  val defaultTopic = "MsgWavesEvent"
}

case object MsgNTFile extends StriderTopic {
  val defaultTopic = "MsgNTFile"
}
