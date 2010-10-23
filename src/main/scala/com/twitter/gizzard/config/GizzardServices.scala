package com.twitter.gizzard.config

import com.twitter.util.Duration

trait GizzardServices {
  def shardServerPort: Int
  def jobServerPort: Int
  def timeout: Duration
}

