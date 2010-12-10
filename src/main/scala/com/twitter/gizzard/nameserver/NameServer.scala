/*
 * Copyright 2010 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.gizzard.nameserver

import java.util.TreeMap
import scala.collection.mutable
import com.twitter.xrayspecs.Time
import com.twitter.xrayspecs.TimeConversions._
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import net.lag.configgy.ConfigMap
import net.lag.logging.Logger
import shards._

class NonExistentShard(message: String) extends ShardException(message: String)
class InvalidShard(message: String) extends ShardException(message: String)

object NameServer {
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
  def apply[S <: shards.Shard](config: ConfigMap, stats: Option[StatsCollector],
                               shardRepository: ShardRepository[S]): NameServer[S] = {
    val queryEvaluatorFactory = QueryEvaluatorFactory.fromConfig(config, stats)

    val replicaConfig = config.configMap("replicas")
    val replicas = replicaConfig.keys.map { key =>
      val shardConfig = replicaConfig.configMap(key)
      shardConfig.getString("type", "mysql") match {
        case "mysql" => new SqlShard(queryEvaluatorFactory(shardConfig))
        case "memory" => new MemoryShard()
      }
    }.collect

    val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.ReplicatingShard", "", "")
    val loadBalancer = new LoadBalancer(replicas)
    val shard = new ReadWriteShardAdapter(
      new ReplicatingShard(shardInfo, 0, replicas, loadBalancer, None))

    val mappingFunction: (Long => Long) = config.getString("mapping", "identity") match {
      case "identity" =>
        { n => n }
      case "byte_swapper" =>
        ByteSwapper
      case "fnv1a-64" =>
        FnvHasher
    }
    new NameServer(shard, shardRepository, mappingFunction)
  }
}

class NameServer[S <: shards.Shard](nameServerShard: Shard, shardRepository: ShardRepository[S],
                                    val mappingFunction: Long => Long)
  extends Shard {

  private val log = Logger.get(getClass.getName)

  val children = List()
  val shardInfo = new ShardInfo("com.twitter.gizzard.nameserver.NameServer", "", "")
  val weight = 1 // hardcode for now
  val RETRIES = 5

  @volatile protected var shardInfos = mutable.Map.empty[ShardId, ShardInfo]
  @volatile private var familyTree: scala.collection.Map[ShardId, Seq[LinkInfo]] = null
  @volatile private var forwardings: scala.collection.Map[Int, TreeMap[Long, ShardInfo]] = null

  @throws(classOf[shards.ShardException])
  def createShard(shardInfo: ShardInfo) {
    nameServerShard.createShard(shardInfo, shardRepository)
  }

  def getShardInfo(id: ShardId) = shardInfos(id)

  def getChildren(id: ShardId) = {
    familyTree.getOrElse(id, new mutable.ArrayBuffer[LinkInfo])
  }

  def reload() {
    log.info("Loading forwarding table...")
    nameServerShard.reload()

    val newShardInfos = mutable.Map.empty[ShardId, ShardInfo]
    nameServerShard.listShards().foreach { shardInfo =>
      newShardInfos += (shardInfo.id -> shardInfo)
    }

    val newFamilyTree = new mutable.HashMap[ShardId, mutable.ArrayBuffer[LinkInfo]]
    nameServerShard.listLinks().foreach { link =>
      val children = newFamilyTree.getOrElseUpdate(link.upId, new mutable.ArrayBuffer[LinkInfo])
      children += link
    }

    val newForwardings = new mutable.HashMap[Int, TreeMap[Long, ShardInfo]]
    nameServerShard.getForwardings().foreach { forwarding =>
      val treeMap = newForwardings.getOrElseUpdate(forwarding.tableId, new TreeMap[Long, ShardInfo])
      treeMap.put(forwarding.baseId, newShardInfos.getOrElse(forwarding.shardId, throw new NonExistentShard("Forwarding (%s) references non-existent shard".format(forwarding))))
    }

    shardInfos = newShardInfos
    familyTree = newFamilyTree
    forwardings = newForwardings
    log.info("Loading forwarding table is done.")
  }

  def findShardById(id: ShardId, weight: Int): S = {
    val shardInfo = getShardInfo(id)
    val children = getChildren(id).map { linkInfo =>
      findShardById(linkInfo.downId, linkInfo.weight)
    }.toList
    shardRepository.find(shardInfo, weight, children)
  }

  @throws(classOf[NonExistentShard])
  def findShardById(id: ShardId): S = findShardById(id, 1)

  def findCurrentForwarding(tableId: Int, id: Long) = {
    val shardInfo = forwardings.get(tableId).flatMap { bySourceIds =>
      val item = bySourceIds.floorEntry(mappingFunction(id))
      if (item != null) {
        Some(item.getValue)
      } else {
        None
      }
    } getOrElse {
      throw new NonExistentShard("No shard for address: %s %s".format(tableId, id))
    }

    findShardById(shardInfo.id)
  }

  @throws(classOf[shards.ShardException]) def createShard[S <: shards.Shard](shardInfo: ShardInfo, repository: ShardRepository[S]) = nameServerShard.createShard(shardInfo, repository)
  @throws(classOf[shards.ShardException]) def getShard(id: ShardId) = nameServerShard.getShard(id)
  @throws(classOf[shards.ShardException]) def deleteShard(id: ShardId) = nameServerShard.deleteShard(id)
  @throws(classOf[shards.ShardException]) def addLink(upId: ShardId, downId: ShardId, weight: Int) = nameServerShard.addLink(upId, downId, weight)
  @throws(classOf[shards.ShardException]) def removeLink(upId: ShardId, downId: ShardId) = nameServerShard.removeLink(upId, downId)
  @throws(classOf[shards.ShardException]) def listUpwardLinks(id: ShardId) = nameServerShard.listUpwardLinks(id)
  @throws(classOf[shards.ShardException]) def listDownwardLinks(id: ShardId) = nameServerShard.listDownwardLinks(id)
  @throws(classOf[shards.ShardException]) def listLinks() = nameServerShard.listLinks()
  @throws(classOf[shards.ShardException]) def markShardBusy(id: ShardId, busy: Busy.Value) = nameServerShard.markShardBusy(id, busy)
  @throws(classOf[shards.ShardException]) def setForwarding(forwarding: Forwarding) = nameServerShard.setForwarding(forwarding)
  @throws(classOf[shards.ShardException]) def replaceForwarding(oldId: ShardId, newId: ShardId) = nameServerShard.replaceForwarding(oldId, newId)
  @throws(classOf[shards.ShardException]) def getForwarding(tableId: Int, baseId: Long) = nameServerShard.getForwarding(tableId, baseId)
  @throws(classOf[shards.ShardException]) def getForwardingForShard(id: ShardId) = nameServerShard.getForwardingForShard(id)
  @throws(classOf[shards.ShardException]) def getForwardings() = nameServerShard.getForwardings()
  @throws(classOf[shards.ShardException]) def shardsForHostname(hostname: String) = nameServerShard.shardsForHostname(hostname)
  @throws(classOf[shards.ShardException]) def listShards() = nameServerShard.listShards()
  @throws(classOf[shards.ShardException]) def getBusyShards() = nameServerShard.getBusyShards()
  @throws(classOf[shards.ShardException]) def getChildShardsOfClass(parentId: ShardId, className: String) = nameServerShard.getChildShardsOfClass(parentId, className)
  @throws(classOf[shards.ShardException]) def rebuildSchema() = nameServerShard.rebuildSchema()
  @throws(classOf[shards.ShardException]) def removeForwarding(f: Forwarding) = nameServerShard.removeForwarding(f)
  @throws(classOf[shards.ShardException]) def listHostnames() = nameServerShard.listHostnames()
}
