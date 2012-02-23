package com.twitter.gizzard.scheduler

import scala.collection.mutable
import com.twitter.gizzard.shards.{ShardBlackHoleException, ShardOfflineException}

/**
 * A wrapper Job for a series of smaller jobs that should be executed together. It will attempt
 * to execute all the smaller jobs, regardless of individual failures. Failed jobs will remain
 * in our queue and can be retried later.
 */
abstract class NestedJob(val jobs: Iterable[JsonJob]) extends JsonJob {
  val taskQueue = {
    val q = new mutable.Queue[JsonJob]()
    q ++= jobs
    q
  }

  def apply() {
    val failedTasks = mutable.Buffer[JsonJob]()
    var lastNormalException: Throwable = null
    var lastOfflineException: ShardOfflineException = null
    var lastBlackHoleException: ShardBlackHoleException = null

    while (!taskQueue.isEmpty) {
      val task: JsonJob = taskQueue.head
      try {
        task.apply()
      } catch {
        case e: ShardBlackHoleException =>
          lastBlackHoleException = e
        case e: ShardOfflineException =>
          failedTasks += task
          lastOfflineException = e
        case e =>
          failedTasks += task
          lastNormalException = e
      }
      taskQueue.dequeue()
    }

    if (!failedTasks.isEmpty) {
      taskQueue ++= failedTasks
    }

    if (lastOfflineException != null) {
      throw lastOfflineException
    } else if (lastNormalException != null) {
      throw lastNormalException
    } else if (lastBlackHoleException != null) {
      throw lastBlackHoleException
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
