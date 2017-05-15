package engine.core.optimizer

import org.apache.log4j.LogManager

/**
  * Created by xiangnanren on 10/10/2016.
  */
abstract class AlgebraOptimizer {
  @transient lazy val log = LogManager.getLogger(this.getClass)
}

