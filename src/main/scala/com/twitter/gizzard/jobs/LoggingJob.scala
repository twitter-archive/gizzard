package com.twitter.gizzard.jobs

import com.twitter.ostrich.W3CStats
import com.twitter.gizzard.proxy.LoggingProxy
import com.twitter.ostrich.StatsProvider


class LoggingJobParser(stats: StatsProvider, w3cStats: W3CStats, jobParser: JobParser) extends JobParser {
  def apply(json: Map[String, Map[String, Any]]) = new LoggingJob(stats, w3cStats, jobParser(json))
}

class LoggingJob(stats: StatsProvider, w3cStats: W3CStats, job: Job) extends JobProxy(job) {
  def apply() { LoggingProxy(stats, w3cStats, job.loggingName, job).apply() }
}
