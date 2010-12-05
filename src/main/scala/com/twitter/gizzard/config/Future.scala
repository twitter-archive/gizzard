package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import net.lag.configgy.ConfigMap

class Future {
  var poolSize    = 1
  var maxPoolSize = 1
  var keepAlive   = 5.seconds
  var timeout     = 1.second

  def apply(name: String) = {
    if (maxPoolSize < poolSize) maxPoolSize = poolSize
    new gizzard.Future(name, poolSize, maxPoolSize, keepAlive, timeout)
  }
}

class ConfiggyFuture(config: ConfigMap) extends Future {
  poolSize    = config("pool_size").toInt
  maxPoolSize = config("max_pool_size").toInt
  keepAlive   = config("keep_alive_time_seconds").toInt.seconds
  timeout     = (config("timeout_seconds").toFloat * 1000).toInt.millis
}
