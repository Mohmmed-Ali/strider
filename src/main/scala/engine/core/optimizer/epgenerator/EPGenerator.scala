package engine.core.optimizer.epgenerator

import engine.core.optimizer.BGPOptimizer
import engine.core.optimizer.ucg.{BGPGraph, UCGraph}

import scala.collection.immutable.List

/**
  * Created by xiangnanren on 06/12/2016.
  */
abstract class EPGenerator extends BGPOptimizer {

  def generate(g: UCGraph): List[BGPGraph]

  def show(ep: List[BGPGraph]): Unit = {
    ep.foreach(_.show())
  }
}
