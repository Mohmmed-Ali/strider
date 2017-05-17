package engine.core.optimizer


/**
  * Created by xiangnanren on 09/03/2017.
  */
trait AdapStrategy {
  val strategy: String
}

object AdapStrategy {

  case object Forward extends AdapStrategy {
    override val strategy: String = "adaptivity.forward"
  }

  case object Backward extends AdapStrategy {
    override val strategy: String = "adaptivity.backward"
  }

}
