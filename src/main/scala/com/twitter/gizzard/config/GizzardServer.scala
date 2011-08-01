package com.twitter.gizzard.config

import com.twitter.logging.config.LoggerConfig
import com.twitter.util.Duration
import com.twitter.conversions.time._

import com.twitter.gizzard.shards
import com.twitter.gizzard.nameserver

trait GizzardServer {
  def jobQueues: Map[Int, Scheduler]

  var loggers: List[LoggerConfig]      = Nil
  var mappingFunction: MappingFunction = Hash
  var nameServerReplicas: Seq[Replica] = Seq(Memory)
  var jobRelay: JobRelay               = new JobRelay
  var manager: Manager                 = new Manager with TThreadServer
  var jobInjector: JobInjector         = new JobInjector with THsHaServer

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

