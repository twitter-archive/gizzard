package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.gizzard.nameserver


class JobRelay {
  var priority: Int     = 0
  var timeout: Duration = 1.seconds
  var retries: Int      = 3

  def apply() = new nameserver.JobRelayFactory(priority, timeout, retries)
}

object NoJobRelay extends JobRelay {
  override def apply() = nameserver.NullJobRelayFactory
}


