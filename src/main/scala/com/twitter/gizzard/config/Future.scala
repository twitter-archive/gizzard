package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap

trait Future {
  def poolSize: Int
  def maxPoolSize: Int
  def keepAlive: Duration
  def timeout: Duration

  def apply(name: String) = {
    new gizzard.Future(name, poolSize, maxPoolSize, keepAlive, timeout)
  }
}

class ConfiggyFuture(config: ConfigMap) {
  val poolSize    = config("pool_size").toInt
  val maxPoolSize = config("max_pool_size").toInt
  val keepAlive   = config("keep_alive_time_seconds").toInt.seconds
  val timeout     = (config("timeout_seconds").toFloat * 1000).toInt.millis
}
