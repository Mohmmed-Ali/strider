package engine.stream

/**
  * Created by xiangnanren on 16/06/2017.
  */
trait KafkaProducerHelper {

  /**
    * This method is used for Kafka producer tuning.
    * By generating the random key for each keyed message, we can spread the
    * data to each partition evenly.
    *
    */
  def createMessageKey(rangeSize: Int): Int = {
    val r = new scala.util.Random
    r.nextInt(rangeSize + 1)
  }

}
