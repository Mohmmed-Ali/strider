package engine.core.sparkop.op

import engine.core.optimizer.ExecutionPlanType
import engine.core.optimizer.ExecutionPlanType._
import engine.core.optimizer.trigger.{AdaptiveTrigger, StaticTrigger, TriggerRules}
import engine.core.optimizer.ucg.{BGPGraph, BGPNode, UCGraph}
import engine.core.sparkop.compiler.SparkOpVisitor
import org.apache.jena.graph
import org.apache.jena.sparql.algebra.op.OpBGP
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.language.implicitConversions

/**
  * Created by xiangnanren on 07/07/16.
  */
class SparkBGP(val opBGP: OpBGP,
               val triples: List[graph.Triple])
  extends SparkOp0 with SparkBGPUtils {
  val ucg = UCGraph(initStaticEP(triples))

  @transient
  private val globalTrigger = TriggerRules.f_primitiveTrigger(triples.length)
  private val static_handler = StaticEPHandler
  private val adap_handler = AdaptiveEPHandler

  override def execute(opName: String,
                       inputDF: DataFrame): SparkOpRes = {
    SparkOpRes(
      computeBGP(computeDFMap(triples, inputDF),inputDF))
  }

  override def visit(sparkOpVisitor: SparkOpVisitor): Unit = {
    sparkOpVisitor.visit(this)
  }

  def update(opName: String,
             inputDF: DataFrame): Unit = ucg.edgeExistence match {
    case true =>
      log.info("Switch to backward adaptivity.")
      adap_handler.currentAdapPlan =
        Option(getAdapPlan(computeDFMap(triples, inputDF)))
    case false =>
  }

  protected def getCurrentEP(dfMap: Map[graph.Triple, DataFrame],
                             inputDF: DataFrame): Seq[BGPGraph] = {
    if (globalTrigger) {
      //TriggerRules.f_trigger(SizeEstimator.estimate(inputDF))
      if (TriggerRules.f_trigger_test) {
        adap_handler.currentAdapPlan match {

          case None => log.info("Switch to forward adaptivity.")
            getAdapPlan(dfMap)

          case Some(oldAdapPlan) => log.debug(
            "\n \n Detailed Query plan info: " +
            "\n" + getEPInfo(oldAdapPlan))
            adap_handler.currentAdapPlan = None
            oldAdapPlan }
      } else getStaticPlan
    } else { getStaticPlan }
  }

  protected def computeBGP(dfMap: Map[graph.Triple, DataFrame],
                           inputDF: DataFrame): DataFrame = {
    computeEP(getCurrentEP(dfMap, inputDF), dfMap)
  }

  private def getStaticPlan: List[BGPGraph] = {
    static_handler.statType match {
      case StaticGreedy => log.info("Activate static greedy plan.")
        static_handler.staticGreedyEP
      case StaticBushy => log.info("Activate static bushy plan.")
        static_handler.staticBushyEP
      case _ => throw new UnsupportedOperationException(
        "Invalid static execution plan.")
    }
  }

  private def getAdapPlan(dfMap: Map[graph.Triple, DataFrame],
                          persistence: Boolean = true): List[BGPGraph] = {
    def setNodesStatistic(dfMap: Map[graph.Triple, DataFrame]): Map[graph.Triple, Long] = dfMap.
      map(tpDF => (tpDF._1, {
        if (persistence)
          tpDF._2.persist(StorageLevel.MEMORY_ONLY).count()
        else tpDF._2.count()
      }
        ))

    ucg.updateWeight(setNodesStatistic(dfMap))
    val trigger = AdaptiveTrigger(ucg)

    adap_handler.adapType match {
      case AdaptiveGreedy => log.info("Activate adaptive greedy plan.")
        trigger.triggerGreedyEP()
      case AdaptiveBushy => log.info("Activate adaptive bushy plan.")
        trigger.triggerBushyEP()
      case _ => throw new UnsupportedOperationException(
        "Invalid adaptive execution plan.")
    }
  }

  /**
    * Initialize the list of BGPNodes without the assignment of statistic weight
    *
    * @param triples : Input triple pattern parsed by Jena ARQ
    * @return All BGPNodes present in current BGP graph
    */
  private def initStaticEP(triples: List[graph.Triple]): List[BGPNode] = {
    triples.map(tp => BGPNode(tp))
  }

  /**
    * A singleton instance groups the
    * configuration for static execution plan.
    */
  @transient
  private object StaticEPHandler {
    private val staticTrigger = StaticTrigger(ucg)
    val statType = getStatType(opSettings)
    val staticGreedyEP = staticTrigger.triggerGreedyEP()
    val staticBushyEP = staticTrigger.triggerBushyEP()

    private def getStatType(opSettings: mutable.HashMap[String, String]):
    ExecutionPlanType = {
      val value = opSettings.
        getOrElse("bgp.staticEP.type", StaticBushy.epType) match {
        case StaticBushy.epType => StaticBushy
        case StaticGreedy.epType => StaticGreedy
        case _ => throw new UnsupportedOperationException(
          "Invalid static execution type.")
      }
      value
    }
  }

  /**
    * A singleton instance groups the
    * configuration for adaptive execution plan.
    */
  @transient
  private object AdaptiveEPHandler {
    val adapType = getAdaptiveType(opSettings)
    // Global variable modifying incurred, maybe
    // another way to avoid this side effect.
    var currentAdapPlan: Option[List[BGPGraph]] = None

    private def getAdaptiveType(opSettings: mutable.HashMap[String, String]):
    ExecutionPlanType = {
      val value = opSettings.
        getOrElse("bgp.adaptiveEP.type", AdaptiveBushy.epType) match {
        case AdaptiveGreedy.epType => AdaptiveGreedy
        case AdaptiveBushy.epType => AdaptiveBushy
        case _ => throw new UnsupportedOperationException(
          "Invalid adaptive execution type.")
      }
      value
    }
  }

}

object SparkBGP {
  def apply(opBGP: OpBGP,
            triples: List[graph.Triple]): SparkBGP = new SparkBGP(opBGP, triples)
}




