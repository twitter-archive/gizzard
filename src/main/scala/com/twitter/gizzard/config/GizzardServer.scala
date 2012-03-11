package com.twitter.gizzard
package config

import com.twitter.logging.Logger
import com.twitter.logging.config.LoggerConfig
import com.twitter.util.Duration
import com.twitter.conversions.time._

import com.twitter.gizzard.shards
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.proxy
import com.twitter.util.StorageUnit
import com.twitter.conversions.storage._
import net.lag.kestrel.config.QueueConfig

trait GizzardServer {
  def jobQueues: Map[Int, Scheduler]

  var loggers: List[LoggerConfig]      = Nil
  var mappingFunction: MappingFunction = Hash
  var nameServerReplicas: Seq[Replica] = Seq(Memory)
  var jobRelay: JobRelay               = new JobRelay
  var manager: Manager                 = new Manager with TThreadServer
  var jobInjector: JobInjector         = new JobInjector with THsHaServer
  var jobAsyncReplicator: JobAsyncReplicator = new JobAsyncReplicator

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

class JobAsyncReplicator {
  var path = "/tmp"
  var maxItems: Int                 = Int.MaxValue
  var maxSize: StorageUnit          = Long.MaxValue.bytes
  var maxItemSize: StorageUnit      = Long.MaxValue.bytes
  var maxAge: Option[Duration]      = None
  var maxJournalSize: StorageUnit   = 16.megabytes
  var maxMemorySize: StorageUnit    = 128.megabytes
  var maxJournalOverflow: Int       = 10
  var discardOldWhenFull: Boolean   = false
  var keepJournal: Boolean          = true
  var syncJournal: Boolean          = false
  var multifileJournal: Boolean     = false
  var expireToQueue: Option[String] = None
  var maxExpireSweep: Int           = Int.MaxValue
  var fanoutOnly: Boolean           = false
  var threadsPerCluster: Int        = 4

  def aConfig = QueueConfig(
    maxItems           = maxItems,
    maxSize            = maxSize,
    maxItemSize        = maxItemSize,
    maxAge             = maxAge,
    maxJournalSize     = maxJournalSize,
    maxMemorySize      = maxMemorySize,
    maxJournalOverflow = maxJournalOverflow,
    discardOldWhenFull = discardOldWhenFull,
    keepJournal        = keepJournal,
    syncJournal        = syncJournal,
    multifileJournal   = multifileJournal,
    expireToQueue      = expireToQueue,
    maxExpireSweep     = maxExpireSweep,
    fanoutOnly         = fanoutOnly
  )

  def apply(jobRelay: => nameserver.JobRelay) = new scheduler.JobAsyncReplicator(jobRelay, aConfig, path, threadsPerCluster)
}

// XXX: move StatsCollection, etc. to separate file
trait TransactionalStatsConsumer {
  def apply(): com.twitter.gizzard.TransactionalStatsConsumer
}

trait HumanReadableTransactionalStatsConsumer extends TransactionalStatsConsumer {
  def loggerName: String
  def apply() = { new com.twitter.gizzard.HumanReadableTransactionalStatsConsumer(loggerName) }
}

trait ConditionalTransactionalStatsConsumer extends TransactionalStatsConsumer {
  def test: TransactionalStatsProvider => Boolean
  def consumer: TransactionalStatsConsumer

  def apply() = { new com.twitter.gizzard.ConditionalTransactionalStatsConsumer(consumer(), test) }
}

trait SlowTransactionalStatsConsumer extends TransactionalStatsConsumer {
  var threshold: Duration = 2.seconds
  var consumer = new HumanReadableTransactionalStatsConsumer { var loggerName = "slow_query" }
  def apply() = { new com.twitter.gizzard.SlowTransactionalStatsConsumer(consumer(), threshold) }
}

trait SampledTransactionalStatsConsumer extends TransactionalStatsConsumer {
  var sampleRate: Double = 0.001
  var consumer = new HumanReadableTransactionalStatsConsumer { var loggerName = "sampled_query" }
  def apply() = { new com.twitter.gizzard.SampledTransactionalStatsConsumer(consumer(), sampleRate) }
}

trait AuditingTransactionalStatsConsumer extends TransactionalStatsConsumer {
  var names: Set[String] = Set()
  var consumer = new HumanReadableTransactionalStatsConsumer { var loggerName = "audit_log" }
  def apply () = { new com.twitter.gizzard.AuditingTransactionalStatsConsumer(consumer(), names) }
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
