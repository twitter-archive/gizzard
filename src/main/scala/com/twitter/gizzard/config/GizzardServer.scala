package com.twitter.gizzard
package config

import com.twitter.logging.Logger
import com.twitter.logging.config.LoggerConfig
import com.twitter.util.Duration
import com.twitter.conversions.time._

import com.twitter.gizzard.shards
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.proxy

trait GizzardServer {
  def jobQueues: Map[Int, Scheduler]

  var loggers: List[LoggerConfig]      = Nil
  var mappingFunction: MappingFunction = Hash
  var nameServerReplicas: Seq[Replica] = Seq(Memory)
  var jobRelay: JobRelay               = new JobRelay
  var manager: Manager                 = new Manager with TThreadServer
  var jobInjector: JobInjector         = new JobInjector with THsHaServer

  var queryStats: StatsCollection = new StatsCollection { }
  var jobStats: StatsCollection = new StatsCollection {
    consumers = Seq(new SlowTransactionalStatsConsumer {
      threshold = 1.minute
      consumer.loggerName = "slow_job"
    })
  }

  def buildNameServer() = {
    val nodes: Seq[nameserver.ShardManagerSource] = nameServerReplicas map {
      case r: Mysql => r(new nameserver.SqlShardManagerSource(_))
      case Memory   => new nameserver.MemoryShardManagerSource
    }

    new nameserver.NameServer(asReplicatingNode(nodes), mappingFunction())
  }

  def buildRemoteClusterManager() = {
    val nodes: Seq[nameserver.RemoteClusterManagerSource] = nameServerReplicas map {
      case r: Mysql => r(new nameserver.SqlRemoteClusterManagerSource(_))
      case Memory   => new nameserver.MemoryRemoteClusterManagerSource
    }

    new nameserver.RemoteClusterManager(asReplicatingNode(nodes), jobRelay())
  }

  private def asReplicatingNode[T](ts: Seq[T]): shards.RoutingNode[T] = {
    new shards.ReplicatingShard(
      new shards.ShardInfo("", "", ""),
      0,
      ts map { shards.LeafRoutingNode(_) }
    )
  }
}

trait Manager extends TServer {
  def name = "GizzardManager"
  var port = 7920
}

trait JobInjector extends TServer {
  def name = "JobInjector"
  var port = 7921
}


// XXX: move StatsCollection, etc. to separate file
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

  def apply[T <: AnyRef](statGrouping: String)(implicit manifest: Manifest[T]): proxy.LoggingProxy[T] = {
    new proxy.LoggingProxy(consumers.map { _() }, statGrouping, name)
  }
  def apply[T <: AnyRef]()(implicit manifest: Manifest[T]): proxy.LoggingProxy[T] = apply("request")
}
