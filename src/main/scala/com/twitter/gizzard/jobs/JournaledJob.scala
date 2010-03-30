package com.twitter.gizzard.jobs

import net.lag.logging.Logger


class JournaledJobParser(jobParser: JobParser, journaller: String => Unit) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = new JournaledJob(jobParser(json), journaller)
}

class JournaledJob(job: Job, journaller: String => Unit) extends JobProxy(job) {
  def apply() {
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
