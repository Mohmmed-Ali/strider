package engine.core.optimizer

/**
  * Created by xiangnanren on 10/11/2016.
  */
sealed trait ExecutionPlanType {
  val epType: String
}

object ExecutionPlanType {

  case object BasicEP extends ExecutionPlanType {
    override val epType: String = "basic"
  }

  case object StaticGreedy extends ExecutionPlanType {
    override val epType = "static.greedy"
  }

  case object StaticBushy extends ExecutionPlanType {
    override val epType = "static.bushy"
  }

  case object AdaptiveGreedy extends ExecutionPlanType {
    override val epType = "adaptive.greedy"
  }

  case object AdaptiveBushy extends ExecutionPlanType {
    override val epType = "adaptive.bushy"
  }

}