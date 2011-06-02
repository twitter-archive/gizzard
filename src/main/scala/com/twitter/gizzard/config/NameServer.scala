package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.conversions.time._
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.querulous.evaluator.QueryEvaluatorFactory

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
  var timeout: Duration = 1.seconds
  var retries: Int      = 3

  def apply() = new nameserver.JobRelayFactory(priority, timeout, retries)
}

object NoJobRelay extends JobRelay {
  override def apply() = nameserver.NullJobRelayFactory
}


trait NameServer {
  var mappingFunction: MappingFunction = Identity
  def replicas: Seq[Replica]
  var jobRelay: JobRelay = new JobRelay

  protected def getMappingFunction(): (Long => Long) = {
    mappingFunction match {
      case gizzard.config.ByteSwapper => nameserver.ByteSwapper
      case gizzard.config.Identity => { n => n }
      case gizzard.config.Fnv1a64 => nameserver.FnvHasher
    }
  }

  def apply[T](shardRepository: nameserver.ShardRepository[T]) = {
    val replicaNodes  = replicas map { replica => new shards.LeafRoutingNode(replica(), 1) }

    val shardInfo     = new shards.ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val replicating   = new shards.ReplicatingShard(shardInfo, 0, replicaNodes)

    new nameserver.NameServer(replicating, shardRepository, jobRelay(), getMappingFunction())
  }
}
