package engine.core.optimizer.trigger


import engine.core.optimizer.AdapStrategy.{Backward, Forward}
import engine.core.optimizer.ExecutionPlanType.{AdaptiveBushy, AdaptiveGreedy, StaticBushy, StaticGreedy}
import engine.core.optimizer.conf.AlgebraOptimizerConf
import engine.core.optimizer.epgenerator.{BushyEPGenerator, GreedyEPGenerator}
import engine.core.optimizer.ucg.{BGPGraph, UCGraph}
import engine.core.optimizer.{AdapStrategy, BGPOptimizer}
import org.apache.log4j.LogManager

import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Created by xiangnanren on 06/12/2016.
  */
sealed abstract class BGPOptimizerTrigger extends BGPOptimizer

case class StaticTrigger(ucg: UCGraph)
  extends BGPOptimizerTrigger {

  /**
    * Apply the node-heuristic execution plan (the execution plan
    * will be presented by a left deep binary tree)
    *
    * @return :
    */
  def triggerGreedyEP(): List[BGPGraph] = {
    log.info("Optimizer of HeuByNode is created. ")
    if (ucg.edgeExistence) GreedyEPGenerator(StaticGreedy).generate(ucg)
    else ucg.ucgNodes
  }

  /**
    * Apply the edge-heuristic execution plan (the execution plan
    * will be presented by a left deep binary tree)
    *
    * @return
    */
  def triggerBushyEP(): List[BGPGraph] = {
    log.info("Optimizer of HeuByEdge is created. ")
    if (ucg.edgeExistence) BushyEPGenerator(StaticBushy).generate(ucg)
    else ucg.ucgNodes
  }
}


case class AdaptiveTrigger(ucg: UCGraph)
  extends BGPOptimizerTrigger {

  /**
    * Apply the node-statistic execution plan (the execution plan
    * will be presented by a left deep binary tree)
    *
    * @return
    */
  def triggerGreedyEP(): List[BGPGraph] = {
    log.info("Optimizer of AdapGreedy is triggered. ")
    GreedyEPGenerator(AdaptiveGreedy).generate(ucg)
  }

  /**
    * Apply the edge-statistic execution plan (the execution plan
    * will be presented by a bushy binary tree)
    *
    * @return
    */
  def triggerBushyEP(): List[BGPGraph] = {
    log.info("Optimizer of AdapBushy is triggered. ")
    BushyEPGenerator(AdaptiveBushy).generate(ucg)
  }
}

/**
  * The singleton instance keeps all the "global" rules
  * which triggers the adaptive optimizer for BGP operator.
  *
  * The "global" refers the rules that are independent query,
  * i.e. only depends on the nature of incoming data stream.
  *
  * For example, the rules for switching the forward and backward
  * optimizer can not be defined as "global". Since the switcher of
  * adaptive strategies depends on current execution of BGP operator,
  * which can not be regarded as a general standard for all the BGP
  * operators.
  */
object TriggerRules {
  @transient
  lazy val log = LogManager.getLogger(this.getClass)
  val dataSizeThreshold: Int =
    getDataSizeThreshold(AlgebraOptimizerConf.settings)
  val adapSwitchThreshold: Double =
    getAdapSwitchThreshold(AlgebraOptimizerConf.settings)
  val f_trigger_test = true
  val f_primitiveTrigger: (Int) => Boolean = {
    case _numBGPs if _numBGPs > 2 => true
    case _numBGPs if _numBGPs <= 2 => false
  }
  val f_adaptiveTrigger: (Long) => Boolean = (workLoad: Long) => {
    dataSizeThreshold match {
      case _workLoad if _workLoad > dataSizeThreshold => true
      case _workLoad if _workLoad <= dataSizeThreshold => false
    }
  }
  val f_switchStrategy: (Double, Long) => AdapStrategy =
    (taskTime: Double, batchSize: Long) => {
      @transient
      val r = taskTime / batchSize

      log.info(
        "taskTime (=" + taskTime +
          ")/batchSize (=" +
          batchSize + ") = " + r)
      if (r >= 0.6) {
        log.info("Take forward adaptivity for the next execution")
        Forward
      }
      else {
        log.info("Take backward adaptivity for the next execution")
        Backward
      }
    }

  private def getDataSizeThreshold(triggerSettings:
                                   mutable.HashMap[String, String]): Int = {
    implicit def stringToInt(x: String): Int = augmentString(x).toInt
    val threshold = triggerSettings.getOrElse(
      "adaptivity.strategySwitch.threshold",
      "300000000")
    threshold
  }

  private def getAdapSwitchThreshold(triggerSettings:
                                     mutable.HashMap[String, String]): Double = {
    implicit def stringToDouble(x: String): Double = augmentString(x).toDouble
    val value = triggerSettings.
      getOrElse(
        "adaptivity.strategySwitch.threshold",
        "0.6"
      ) match {
      case t if t <= 1 && t >= 0 => t
      case _ =>
        throw new NumberFormatException("The threshold should be in [0, 1]")
    }
    value
  }
}

