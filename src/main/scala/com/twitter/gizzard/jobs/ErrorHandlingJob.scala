package com.twitter.gizzard.jobs

import com.twitter.ostrich.Stats
import net.lag.logging.Logger
import shards.ShardRejectedOperationException
import scheduler.{ErrorHandlingConfig, MessageQueue, Scheduler}


class ErrorHandlingJobParser(config: ErrorHandlingConfig, errorJobQueue: Scheduler[Schedulable])
  extends JobParser {

  def apply(json: Map[String, Map[String, Any]]) = {
    val (_, attributes) = json.toList.first
    val job = config.jobParser(json)
    new ErrorHandlingJob(job, attributes.getOrElse("error_count", 0).asInstanceOf[Int],
                         errorJobQueue, config)
  }
}

class ErrorHandlingJob(job: Job, var errorCount: Int,
                       errorJobQueue: Scheduler[Schedulable], config: ErrorHandlingConfig)
  extends JobProxy(job) {

  private val log = Logger.get(getClass.getName)
  val (errorLimit, badJobQueue) = (config.errorLimit, config.badJobQueue)

  def apply() {
    try {
      job()
      Stats.incr("job-success-count")
    } catch {
      case e: ShardRejectedOperationException =>
        Stats.incr("job-darkmoded-count")
        errorJobQueue.put(this)
      case e =>
        Stats.incr("job-error-count")
        log.error(e, "Error in Job: " + e)
        errorCount += 1
        if (errorCount > errorLimit) {
          badJobQueue.put(this)
        } else {
          errorJobQueue.put(this)
        }
    }
  }

  override def toMap = job.toMap ++ Map("error_count" -> errorCount)
  override def toString = "ErrorHandlingJob(%s, %s, %d)".format(job, errorJobQueue, errorCount)
}
