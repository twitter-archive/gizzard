package com.twitter.gizzard.config

import com.twitter.logging.config.LoggerConfig
import com.twitter.querulous.config.QueryEvaluator
import com.twitter.util._
import com.twitter.util.Duration
import com.twitter.conversions.time._

trait GizzardServer {
  var loggers: List[LoggerConfig] = Nil
  def jobQueues: Map[Int, Scheduler]
  def nameServer: NameServer

  var jobRelay: JobRelay       = new JobRelay
  var manager: Manager         = new Manager with TThreadServer
  var jobInjector: JobInjector = new JobInjector with THsHaServer
}

trait Manager extends TServer {
  def name = "GizzardManager"
  var port = 7920
}

trait JobInjector extends TServer {
  def name = "JobInjector"
  var port = 7921
}

