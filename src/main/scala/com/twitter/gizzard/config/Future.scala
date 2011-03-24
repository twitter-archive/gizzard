package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

import com.twitter.gizzard

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
