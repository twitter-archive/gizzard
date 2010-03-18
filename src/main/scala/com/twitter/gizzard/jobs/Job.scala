package com.twitter.gizzard.jobs

import com.twitter.json.Json
import net.lag.configgy.Configgy


trait Job extends Schedulable {
  def apply()
}

abstract class JobProxy(job: Job) extends SchedulableProxy(job) with Job