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
  var queryEvaluator: QueryEvaluator = new QueryEvaluator

  def apply() = new SqlShard(queryEvaluator()(connection))
}

object Memory extends Replica {
  def apply() = new MemoryShard
}


class JobRelay {
  var priority: Int     = 0
  var framed: Boolean   = true
  var timeout: Duration = 1.seconds
  var retries: Int      = 3

  def apply() = new JobRelayFactory(priority, framed, timeout, retries)
}

object NoJobRelay extends JobRelay {
  override def apply() = NullJobRelayFactory
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

  def apply[S <: shards.Shard](shardRepository: ShardRepository[S]) = {
    val replicaShards = replicas.map(_.apply())
    val shardInfo     = new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val loadBalancer  = new LoadBalancer(replicaShards)
    val shard  = new ReadWriteShardAdapter(
      new ReplicatingShard(shardInfo, 0, replicaShards, loadBalancer, None))

    new nameserver.NameServer(shard, shardRepository, jobRelay(), getMappingFunction)
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
  mappingFunction = config.getString("mapping", "identity") match {
    case "identity"     => Identity
    case "byte_swapper" => ByteSwapper
    case "fnv1a-64"     => Fnv1a64
  }

  jobRelay = config.getConfigMap("job_relay").map { relayConfig =>
    new JobRelay {
      priority = relayConfig.getInt("priority").get
      framed   = relayConfig.getBool("framed_transport", false)
      timeout  = relayConfig.getInt("timeout_msec", 1000).millis
    }
  } getOrElse NoJobRelay

  val replicas = {
    val replicaConfig = config.configMap("replicas")

    replicaConfig.keys.map { key =>
      val shardConfig = replicaConfig.configMap(key)
      shardConfig.getString("type", "mysql") match {
        case "memory" => Memory
        case "mysql"  => new Mysql {
          queryEvaluator = new ConfiggyQueryEvaluator(config)
          val connection = new ConfiggyConnection(shardConfig)
        }
      }
    }
  }.collect
}
