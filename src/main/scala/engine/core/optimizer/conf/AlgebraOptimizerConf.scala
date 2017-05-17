package engine.core.optimizer.conf

import scala.collection.mutable

/**
  * Created by xiangnanren on 17/03/2017.
  */
/**
  * The global configuration for algebra
  * optimizer configuration
  */
object AlgebraOptimizerConf {
  @transient
  lazy val settings = new mutable.HashMap[String, String]
  val opSettingsKey = Seq(
    "bgp.adaptive.threshold",
    "bgp.staticEP.type",
    "bgp.adaptiveEP.type",
    "adaptivity.strategySwitch.threshold")

  def set(key: String, value: String): AlgebraOptimizerConf.type = {
    if (key == null) {
      throw new NullPointerException("Null key for settings")
    }
    if (!opSettingsKey.contains(key)) {
      throw new NullPointerException("Invalid key for settings")
    }
    if (value == null) {
      throw new NullPointerException("Null value for settings")
    }
    settings.put(key, value)
    this
  }
}

