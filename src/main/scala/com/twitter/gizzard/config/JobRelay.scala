package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.gizzard.nameserver


class JobRelay {
  var priority: Int     = 0
  var timeout: Duration = 500.millis
  var requestTimeout: Duration = 300.millis
  var retries: Int      = 3

  def apply() = new nameserver.JobRelayFactory(priority, timeout, requestTimeout, retries)
}

object NoJobRelay extends JobRelay {
  override def apply() = nameserver.NullJobRelayFactory
}
