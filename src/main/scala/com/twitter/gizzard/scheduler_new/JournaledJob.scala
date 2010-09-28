package com.twitter.gizzard.scheduler

import net.lag.logging.Logger

/**
 * Wrapper for JsonJob that logs jobs after they are successfully executed.
 */
class JournaledJob(val job: JsonJob, journaller: String => Unit) extends JobProxy with JsonJob {
  def toMap = job.toMap

  override def apply() {
    job()
    try {
      journaller(job.toJson)
    } catch {
      case e: Exception =>
        val log = Logger.get(getClass.getName)
        log.warning(e, "Failed to journal job: %s", job.toJson)
    }
  }
}
