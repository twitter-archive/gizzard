package com.twitter.gizzard.scheduler

import net.lag.logging.Logger

/**
 * Wrapper for JsonJob that logs jobs after they are successfully executed.
 */
class JournaledJob[E](job: JsonJob[E], journaller: String => Unit) extends JsonJob[E] {
  def toMap = job.toMap
  val environment = job.environment

  def apply(environment: E) {
    job(environment)
    try {
      journaller(job.toJson)
    } catch {
      case e: Exception =>
        val log = Logger.get(getClass.getName)
        log.warning(e, "Failed to journal job: %s", job.toJson)
    }
  }
}
