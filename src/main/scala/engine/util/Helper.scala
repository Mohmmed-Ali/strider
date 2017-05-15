package engine.util

import scala.collection.parallel.ForkJoinTaskSupport
import scala.collection.parallel.mutable.ParArray

/**
  * Created by xiangnanren on 07/03/2017.
  */
object Helper {

  def displayRunTime[A](task: => A): Unit = {
    val tStart = System.nanoTime()
    task
    println("The execution time is: " +
      (System.nanoTime() - tStart) / 1e6 + "ms")
  }

  def getRunTime[A](task: => A): Double = {
    val tStart = System.nanoTime()
    task
    (System.nanoTime() - tStart) / 1e6
  }

  def getQueryPool(numTasks: Int): ParArray[Int] = {
    val pool = (1 to numTasks).toParArray
    pool.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(1))
    pool
  }
}



