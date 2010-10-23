package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.querulous.config.Connection
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import nameserver._
import shards.{ReplicatingShard, ShardInfo}


trait MappingFunction
object ByteSwapper extends MappingFunction
object Identity extends MappingFunction
object Fnv1a64 extends MappingFunction

trait Replica
trait Mysql extends Replica with Connection
object Memory extends Replica

trait NameServer {
  def mappingFunction: MappingFunction
  def replicas: Seq[Replica]
  def writeTimeout: Duration

  protected def getMappingFunction: (Long => Long) = {
    mappingFunction match {
      case gizzard.config.ByteSwapper => nameserver.ByteSwapper
      case gizzard.config.Identity => { n => n }
      case gizzard.config.Fnv1a64 => nameserver.FnvHasher
    }
  }

  def apply[S <: shards.Shard](queryEvaluatorFactory: QueryEvaluatorFactory,
                               shardRepository: ShardRepository[S],
                               replicationFuture: Option[gizzard.Future]) = {
    val replicaShards = replicas.map { replica =>
      replica match {
        case x: gizzard.config.Mysql => new SqlShard(queryEvaluatorFactory(x))
        case gizzard.config.Memory => new MemoryShard
      }
    }

    val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val loadBalancer = new LoadBalancer(replicaShards)
    val shard = new ReadWriteShardAdapter(
      new ReplicatingShard(shardInfo, 0, replicaShards, loadBalancer, replicationFuture, writeTimeout))

    new nameserver.NameServer(shard, shardRepository, getMappingFunction)
  }
}
