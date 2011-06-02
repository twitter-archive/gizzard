package com.twitter.gizzard
package config

import com.twitter.logging.Logger
import com.twitter.logging.config.LoggerConfig
import com.twitter.querulous.config.QueryEvaluator
import com.twitter.util._
import com.twitter.util.Duration
import com.twitter.util.TimeConversions._

import proxy.LoggingProxy

trait GizzardServer {
  var loggers: List[LoggerConfig] = Nil
  def jobQueues: Map[Int, Scheduler]
  def nameServer: NameServer

  var manager: Manager         = new Manager with TThreadServer
  var jobInjector: JobInjector = new JobInjector with THsHaServer

  var queryStats: StatsCollection = new StatsCollection { }
  var jobStats: StatsCollection = new StatsCollection {
    slowQueryThreshold = 1.minute
    slowQueryLoggerName = "slow_job"
    sampledQueryLoggerName = "sampled_job"
  }
}

trait StatsCollection {
  var name: Option[String] = None
  def name_=(n: String) { name = Some(n) }

  var slowQueryThreshold: Duration = 2.seconds
  var slowQueryLoggerName: String = "slow_query"

  var sampledQueryRate: Double = 0.0
  var sampledQueryLoggerName: String = "sampled_query"

  private def makeStatsConsumers = {
    val sampledQueryConsumer = new SampledTransactionalStatsConsumer(
      new LoggingTransactionalStatsConsumer(Logger.get(sampledQueryLoggerName)), sampledQueryRate)
    val slowQueryConsumer = new SlowTransactionalStatsConsumer(
      new LoggingTransactionalStatsConsumer(Logger.get(slowQueryLoggerName)), slowQueryThreshold.inMillis)
    Seq(sampledQueryConsumer, slowQueryConsumer)
  }

  def apply[T <: AnyRef](statGrouping: String)(implicit manifest: Manifest[T]): LoggingProxy[T] = {
    new proxy.LoggingProxy(makeStatsConsumers, statGrouping, name)
  }
  def apply[T <: AnyRef]()(implicit manifest: Manifest[T]): LoggingProxy[T] = apply("request")
}

trait Manager extends TServer {
  def name = "GizzardManager"
  var port = 7920
}

trait JobInjector extends TServer {
  def name = "JobInjector"
  var port = 7921
}

