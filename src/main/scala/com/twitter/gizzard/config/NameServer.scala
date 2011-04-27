package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.querulous.config.{Connection, QueryEvaluator, ConfiggyConnection, ConfiggyQueryEvaluator}
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import net.lag.configgy.ConfigMap

import com.twitter.gizzard
import com.twitter.gizzard.nameserver
import com.twitter.gizzard.shards
import com.twitter.gizzard.shards.{ReplicatingShard, ShardInfo}


trait MappingFunction
object ByteSwapper extends MappingFunction
object Identity extends MappingFunction
object Fnv1a64 extends MappingFunction

trait Replica {
  def apply(): nameserver.Shard
}

trait Mysql extends Replica {
  def connection: Connection
  var queryEvaluator: QueryEvaluator = new QueryEvaluator

  def apply() = new nameserver.SqlShard(queryEvaluator()(connection))
}

object Memory extends Replica {
  def apply() = new nameserver.MemoryShard
}


class JobRelay {
  var priority: Int     = 0
  var framed: Boolean   = true
  var timeout: Duration = 1.seconds
  var retries: Int      = 3

  def apply() = new nameserver.JobRelayFactory(priority, framed, timeout, retries)
}

object NoJobRelay extends JobRelay {
  override def apply() = nameserver.NullJobRelayFactory
}


trait NameServer {
  var mappingFunction: MappingFunction = Identity
  def replicas: Seq[Replica]
  var jobRelay: JobRelay = new JobRelay

  protected def getMappingFunction: (Long => Long) = {
    mappingFunction match {
      case gizzard.config.ByteSwapper => nameserver.ByteSwapper
      case gizzard.config.Identity => { n => n }
      case gizzard.config.Fnv1a64 => nameserver.FnvHasher
    }
  }

  def apply[S <: shards.Shard](shardRepository: nameserver.ShardRepository[S]) = {
    val replicaShards = replicas.map(_.apply())
    val shardInfo     = new shards.ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val loadBalancer  = new nameserver.LoadBalancer(replicaShards)
    val shard  = new nameserver.ReadWriteShardAdapter(
      new shards.ReplicatingShard(shardInfo, 0, replicaShards, loadBalancer, None))

    new nameserver.NameServer(shard, shardRepository, jobRelay(), getMappingFunction)
  }
}
