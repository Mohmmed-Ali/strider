package engine.core.optimizer

import engine.core.optimizer.ucg.{BGPGraph, BGPNode}
import engine.core.sparkop.op.SparkRes

import scala.collection.mutable

/**
  * Created by xiangnanren on 10/11/2016.
  */
sealed abstract class BGPExecutionPlan {
  def show(): Unit
}

case class StaticEP(ep: Seq[BGPNode]) extends BGPExecutionPlan {
  override def show(): Unit = ep.foreach(node => node.show())
}

case class DynamicEP(ep: Seq[BGPGraph]) extends BGPExecutionPlan {
  val stack = new mutable.Stack[SparkRes]

  override def show(): Unit = ep.foreach(node => node.show())
}


