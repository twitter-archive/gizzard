package com.twitter.gizzard.jobs

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
  val (errorLimit, badJobQueue, stats) = (config.errorLimit, config.badJobQueue, config.stats)

  def apply() {
    try {
      job()
      stats.incr("job-success-count", 1)
    } catch {
      case e: ShardRejectedOperationException =>
        stats.incr("job-darkmoded-count", 1)
        errorJobQueue.put(this)
      case e =>
        stats.incr("job-error-count", 1)
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
