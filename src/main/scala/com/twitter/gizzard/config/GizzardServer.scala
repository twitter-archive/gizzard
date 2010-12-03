package com.twitter.gizzard.config

import com.twitter.querulous.config.QueryEvaluator
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

trait GizzardServer {
  def logging: Logging = NoLogging
  def jobQueues: Map[Int, Scheduler]
  def nameServer: NameServer

  val manager = new Manager with TThreadServer {
    val threadPool   = new ThreadPool {
      val name       = "gizzard"
      val minThreads = 0
      override val maxThreads = 1
    }
  }

  def jobInjector: JobInjector
}

trait Manager extends TServer {
  val name        = "GizzardManager"
  val port        = 7920
  val timeout     = 60.milliseconds
  val idleTimeout = 100.seconds
}

trait JobInjector extends TServer {
  val name = "JobInjector"
  val port = 7921
}
