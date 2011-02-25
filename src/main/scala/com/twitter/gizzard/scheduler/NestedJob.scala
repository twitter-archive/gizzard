package com.twitter.gizzard.scheduler

import scala.collection.mutable

/**
 * A wrapper Job for a series of smaller jobs that should be executed together in series.
 * If any of the smaller jobs throws an exception, the NestedJob is enqueued with only that
 * job and the remaining jobs -- in other words, it's enqueued with its progress so far.
 */
abstract class NestedJob(val jobs: Iterable[JsonJob]) extends JsonJob {
  val taskQueue = {
    val q = new mutable.Queue[JsonJob]()
    q ++= jobs
    q
  }

  def apply() {
    while (!taskQueue.isEmpty) {
      taskQueue.head.apply()
      taskQueue.dequeue()
    }
  }

  override def loggingName = jobs.map { _.loggingName }.mkString(",")

  override def equals(other: Any) = {
    other match {
      case other: NestedJob if (other ne null) =>
        taskQueue.toList == other.taskQueue.toList
      case _ =>
        false
    }
  }

  override def toString = "<NestedJob: tasks=%d: %s>".format(taskQueue.size, jobs)
}
