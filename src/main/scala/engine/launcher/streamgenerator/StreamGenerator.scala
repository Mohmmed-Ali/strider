package engine.launcher.streamgenerator

import org.apache.log4j.LogManager

/**
  * Created by xiangnanren on 09/12/2016.
  */
abstract class StreamGenerator extends Serializable {
  @transient lazy val log = LogManager.getLogger(this.getClass)

  def generate()
}




