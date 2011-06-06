package com.twitter.gizzard
package config

import com.twitter.logging.Logger
import com.twitter.logging.config.LoggerConfig
import com.twitter.querulous.config.QueryEvaluator
import com.twitter.util._
import com.twitter.util.Duration
import com.twitter.conversions.time._

import proxy.LoggingProxy

trait GizzardServer {
  var loggers: List[LoggerConfig] = Nil
  def jobQueues: Map[Int, Scheduler]
  def nameServer: NameServer

  var manager: Manager         = new Manager with TThreadServer
  var jobInjector: JobInjector = new JobInjector with THsHaServer

  var queryStats: StatsCollection = new StatsCollection { }
  var jobStats: StatsCollection = new StatsCollection {
    consumers = Seq(new SlowTransactionalStatsConsumer {
      threshold = 1.minute
      consumer.loggerName = "slow_job"
    })
  }
}

trait TransactionalStatsConsumer {
  def apply(): com.twitter.gizzard.TransactionalStatsConsumer
}

trait LoggingTransactionalStatsConsumer extends TransactionalStatsConsumer {
  def loggerName: String
  def apply() = { new com.twitter.gizzard.LoggingTransactionalStatsConsumer(loggerName) }
}

trait ConditionalTransactionalStatsConsumer extends TransactionalStatsConsumer {
  def test: TransactionalStatsProvider => Boolean
  def consumer: TransactionalStatsConsumer

  def apply() = { new com.twitter.gizzard.ConditionalTransactionalStatsConsumer(consumer(), test) }
}

trait SlowTransactionalStatsConsumer extends TransactionalStatsConsumer {
  var threshold: Duration = 2.seconds
  var consumer = new LoggingTransactionalStatsConsumer { var loggerName = "slow_query" }
  def apply() = { new com.twitter.gizzard.SlowTransactionalStatsConsumer(consumer(), threshold) }
}

trait SampledTransactionalStatsConsumer extends TransactionalStatsConsumer {
  var sampleRate: Double = 0.001
  var consumer = new LoggingTransactionalStatsConsumer { var loggerName = "sampled_query" }
  def apply() = { new com.twitter.gizzard.SampledTransactionalStatsConsumer(consumer(), sampleRate) }
}

trait StatsCollection {
  var name: Option[String] = None
  def name_=(n: String) { name = Some(n) }

  var consumers: Seq[TransactionalStatsConsumer] = Seq(new SlowTransactionalStatsConsumer {})

  def apply[T <: AnyRef](statGrouping: String)(implicit manifest: Manifest[T]): LoggingProxy[T] = {
    new proxy.LoggingProxy(consumers.map { _() }, statGrouping, name)
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

