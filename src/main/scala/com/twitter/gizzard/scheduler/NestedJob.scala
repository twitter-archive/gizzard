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
    var lastNormalException: Option[Throwable] = None
    var lastOfflineException: Option[ShardOfflineException] = None
    var lastBlackHoleException: Option[ShardBlackHoleException] = None

    while (!taskQueue.isEmpty) {
      val task = taskQueue.head
      try {
        task.apply()
      } catch {
        case e: ShardBlackHoleException =>
          lastBlackHoleException = Some(e)
        case e: ShardOfflineException =>
          failedTasks += task
          lastOfflineException = Some(e)
        case e =>
          failedTasks += task
          lastNormalException = Some(e)
      }
      taskQueue.dequeue()
    }

    if (!failedTasks.isEmpty) {
      taskQueue ++= failedTasks
    }

    // Prioritize exceptions:
    //  (1) ShardOfflineException - jobs with this exception should be retried forever
    //  (2) Regular Exception 
    //  (3) ShardBlackHoleException - jobs with this exception will not be retried
    lastOfflineException orElse lastNormalException orElse lastBlackHoleException foreach { e =>
      throw e
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
