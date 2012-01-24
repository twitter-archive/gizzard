package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.gizzard.nameserver
import net.lag.kestrel.config.QueueConfig

class JobRelay {
  var queueConfig = QueueConfig(
    maxItems                   = Int.MaxValue,
    maxSize                    = Long.MaxValue.bytes,
    maxItemSize                = Long.MaxValue.bytes,
    maxAge                     = None,
    maxJournalSize             = 16.megabytes,
    maxMemorySize              = 128.megabytes,
    maxJournalOverflow         = 10,
    discardOldWhenFull         = false,
    keepJournal                = true,
    syncJournal                = false,
    multifileJournal           = false,
    expireToQueue              = None,
    maxExpireSweep             = Int.MaxValue,
    fanoutOnly                 = false
  )

  var priority: Int     = 0
  var timeout: Duration = 500.millis
  var requestTimeout: Duration = 300.millis
  var retries: Int      = 3
  var queueRoot : String = "/tmp"

  def apply() = new nameserver.JobRelayFactory(priority, timeout, requestTimeout, retries, queueConfig, queueRoot)
}

object NoJobRelay extends JobRelay {
  override def apply() = nameserver.NullJobRelayFactory
}
