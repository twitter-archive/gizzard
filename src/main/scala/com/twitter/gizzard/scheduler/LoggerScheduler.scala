package com.twitter.gizzard.scheduler

import net.lag.logging.Logger


class LoggerScheduler(logger: Logger) extends Scheduler[String] {
  def put(value: String) = logger.error(value)
}