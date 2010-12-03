package com.twitter.gizzard.config

import com.twitter.util.Duration
import com.twitter.util.TimeConversions._
import com.twitter.querulous.config.{Connection, QueryEvaluator, ConfiggyConnection, ConfiggyQueryEvaluator}
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import nameserver._
import shards.{ReplicatingShard, ShardInfo}
import net.lag.configgy.ConfigMap


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

  def apply[S <: shards.Shard](shardRepository: ShardRepository[S]) = {

    val replicaShards = replicas.map(_.apply())
    val shardInfo     = new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val loadBalancer  = new LoadBalancer(replicaShards)
    val shard  = new ReadWriteShardAdapter(
      new ReplicatingShard(shardInfo, 0, replicaShards, loadBalancer, None))

    new nameserver.NameServer(shard, shardRepository, getJobRelay, getMappingFunction)
  }
}


/**
 * nameserver (inherit="db") {
 *   mapping = "byte_swapper"
 *   replicas {
 *     ns1 (inherit="db") {
 *       type = "mysql"
 *       hostname = "nameserver1"
 *       database = "shards"
 *     }
 *     ns2 (inherit="db") {
 *       hostname = "nameserver2"
 *       database = "shards"
 *     }
 *   }
 * }
 */

class ConfiggyNameServer(config: ConfigMap) extends NameServer {
  val mappingFunction = config.getString("mapping", "identity") match {
    case "identity"     => Identity
    case "byte_swapper" => ByteSwapper
    case "fnv1a-64"     => Fnv1a64
  }

  val jobRelay = config.getConfigMap("job_relay").map { relayConfig =>
    new JobRelay {
      val priority = relayConfig.getInt("priority").get
      val framed   = relayConfig.getBool("framed_transport", false)
      val timeout  = relayConfig.getInt("timeout_msec", 1000).millis
    }
  }

  val replicas = {
    val replicaConfig = config.configMap("replicas")

    replicaConfig.keys.map { key =>
      val shardConfig = replicaConfig.configMap(key)
      shardConfig.getString("type", "mysql") match {
        case "memory" => Memory
        case "mysql"  => new Mysql {
          val queryEvaluator = new ConfiggyQueryEvaluator(config)
          val connection     = new ConfiggyConnection(shardConfig)
        }
      }
    }
  }.collect
}
