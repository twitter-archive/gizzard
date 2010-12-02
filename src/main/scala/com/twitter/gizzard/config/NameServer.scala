package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.querulous.config.{Connection, QueryEvaluator}
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import nameserver._
import shards.{ReplicatingShard, ShardInfo}


trait MappingFunction
object ByteSwapper extends MappingFunction
object Identity extends MappingFunction
object Fnv1a64 extends MappingFunction

trait Replica {
  def apply(): nameserver.Shard
}

trait Mysql extends Replica {
  def connection: Connection
  def queryEvaluator: QueryEvaluator

  def apply() = new SqlShard(queryEvaluator()(connection))
}

object Memory extends Replica {
  def apply() = new MemoryShard
}


trait JobRelay {
  def priority: Int
  def framed: Boolean
  def timeout: Duration

  def apply() = new nameserver.JobRelayFactory(priority, framed, timeout)
}

trait NameServer {
  def mappingFunction: MappingFunction
  def replicas: Seq[Replica]
  def jobRelay: Option[JobRelay]

  protected def getMappingFunction: (Long => Long) = {
    mappingFunction match {
      case gizzard.config.ByteSwapper => nameserver.ByteSwapper
      case gizzard.config.Identity => { n => n }
      case gizzard.config.Fnv1a64 => nameserver.FnvHasher
    }
  }

  protected def getJobRelay = jobRelay match {
    case Some(relay) => relay()
    case None        => NullJobRelayFactory
  }

  def apply[S <: shards.Shard](shardRepository: ShardRepository[S],
                               replicationFuture: Option[gizzard.Future]) = {

    val replicaShards = replicas.map(_.apply())
    val shardInfo     = new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val loadBalancer  = new LoadBalancer(replicaShards)
    val shard  = new ReadWriteShardAdapter(
      new ReplicatingShard(shardInfo, 0, replicaShards, loadBalancer, replicationFuture))

    new nameserver.NameServer(shard, shardRepository, getJobRelay, getMappingFunction)
  }
}
