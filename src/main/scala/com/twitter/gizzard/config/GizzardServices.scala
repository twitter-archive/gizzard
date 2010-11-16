package com.twitter.gizzard.config

import com.twitter.util.Duration

trait GizzardServices {
  def managerServerPort: Int
  def timeout: Duration
}

