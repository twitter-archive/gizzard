package com.twitter.gizzard.config

import com.twitter.util.Duration


trait Future {
  val poolSize: Int
  val maxPoolSize: Int
  val keepAlive: Duration
  val timeout: Duration
}

