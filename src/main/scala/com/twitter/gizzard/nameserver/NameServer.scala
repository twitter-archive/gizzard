package com.twitter.gizzard.nameserver

import java.util.TreeMap
import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.gizzard.shards._


class NonExistentShard(message: String) extends ShardException(message: String)
class InvalidShard(message: String) extends ShardException(message: String)
class NameserverUninitialized extends ShardException("Please call reload() before operating on the NameServer")

class NameServerSource(shard: RoutingNode[ShardManagerSource]) {
  def reload()       { shard.write.foreach(_.reload()) }
  def currentState() = shard.read.any(_.currentState())
}

class NameServer(val shard: RoutingNode[ShardManagerSource], val mappingFunction: Long => Long) {

  val shardRepository = new ShardRepository

  // XXX: inject these in later
  val source       = new NameServerSource(shard)
  val shardManager = new ShardManager(shard)

  private val log = Logger.get(getClass.getName)

  @volatile protected var shardInfos = mutable.Map.empty[ShardId, ShardInfo]
  @volatile private var familyTree: scala.collection.Map[ShardId, Seq[LinkInfo]] = null
  @volatile private var forwardings: scala.collection.Map[Int, TreeMap[Long, ShardInfo]] = null

  import ForwarderBuilder._

  def forwarder[T : Manifest] = {
    shardRepository.singleTableForwarder[T]
  }

  def multiTableForwarder[T : Manifest] = {
    shardRepository.multiTableForwarder[T]
  }

  def configureForwarder[T : Manifest](config: SingleForwarderBuilder[T, No, No] => SingleForwarderBuilder[T, Yes, Yes]) = {
    val forwarder = config(ForwarderBuilder.singleTable[T]).build(this)
    shardRepository.addSingleTableForwarder(forwarder)
    forwarder
  }

  def configureMultiForwarder[T : Manifest](config: MultiForwarderBuilder[T, Yes, No] => MultiForwarderBuilder[T, Yes, Yes]) = {
    val forwarder = config(ForwarderBuilder.multiTable[T]).build(this)
    shardRepository.addMultiTableForwarder(forwarder)
    forwarder
  }

  def newCopyJob(from: ShardId, to: ShardId) = {
    shardRepository.newCopyJob(from, to)
  }

  def newRepairJob(ids: Seq[ShardId]) = {
    shardRepository.newRepairJob(ids)
  }

  def newDiffJob(ids: Seq[ShardId]) = {
    shardRepository.newDiffJob(ids)
  }

  @throws(classOf[ShardException])
  def createAndMaterializeShard(shardInfo: ShardInfo) {
    shardManager.createShard(shardInfo)
    shardRepository.materializeShard(shardInfo)
  }

  def getShardInfo(id: ShardId) = shardInfos(id)

  def getChildren(id: ShardId) = {
    if(familyTree == null) throw new NameserverUninitialized
    familyTree.getOrElse(id, new mutable.ArrayBuffer[LinkInfo])
  }

  private def recreateInternalShardState() {
    val newShardInfos     = mutable.Map[ShardId, ShardInfo]()
    val newFamilyTree     = mutable.Map[ShardId, mutable.ArrayBuffer[LinkInfo]]()
    val newForwardings    = mutable.Map[Int, TreeMap[Long, ShardInfo]]()

    source.currentState().foreach { state =>

      state.shards.foreach { info => newShardInfos += (info.id -> info) }

      state.links.foreach { link =>
        newFamilyTree.getOrElseUpdate(link.upId, new mutable.ArrayBuffer[LinkInfo]) += link
      }

      state.forwardings.foreach { forwarding =>
        val treeMap = newForwardings.getOrElseUpdate(forwarding.tableId, new TreeMap[Long, ShardInfo])

        newShardInfos.get(forwarding.shardId) match {
          case Some(shard) => treeMap.put(forwarding.baseId, shard)
          case None => {
            throw new NonExistentShard("Forwarding (%s) references non-existent shard".format(forwarding))
          }
        }
      }
    }

    shardInfos  = newShardInfos
    familyTree  = newFamilyTree
    forwardings = newForwardings
  }

  def reloadUpdatedForwardings() {
    log.info("Loading updated name server configuration...")
    recreateInternalShardState()
    log.info("Loading updated name server configuration is done.")
  }

  def reload() {
    log.info("Loading name server configuration...")
    source.reload()
    recreateInternalShardState()
    log.info("Loading name server configuration is done.")
  }

  // XXX: removing this causes CopyJobSpec to fail.
  @throws(classOf[NonExistentShard])
  def findShardById[T](id: ShardId, weight: Int): RoutingNode[T] = {
    val (shardInfo, downwardLinks) = shardInfos.get(id).map { info =>
      // either pull shard and links from our internal data structures...
      (info, getChildren(id))
    } getOrElse {
      // or directly from the db, in the case they are not attached to a forwarding.
      (shardManager.getShard(id), shardManager.listDownwardLinks(id))
    }

    val children = downwardLinks.map(l => findShardById[T](l.downId, l.weight)).toList

    shardRepository.instantiateNode[T](shardInfo, weight, children)
  }

  // XXX: removing this causes CopyJobSpec to fail.
  @throws(classOf[NonExistentShard])
  def findShardById[T](id: ShardId): RoutingNode[T] = findShardById(id, 1)

  @throws(classOf[NonExistentShard])
  def findCurrentForwarding[T](tableId: Int, id: Long): RoutingNode[T] = {
    if(forwardings == null) throw new NameserverUninitialized
    val shardInfo = forwardings.get(tableId) flatMap { bySourceIds =>
      val item = bySourceIds.floorEntry(mappingFunction(id))
      if (item != null) {
        Some(item.getValue)
      } else {
        None
      }
    } getOrElse {
      throw new NonExistentShard("No shard for address: %s %s".format(tableId, id))
    }

    findShardById[T](shardInfo.id)
  }

  @throws(classOf[NonExistentShard])
  def findForwardings[T](tableId: Int): Seq[RoutingNode[T]] = {
    import scala.collection.JavaConversions._

    if(forwardings == null) throw new NameserverUninitialized
    forwardings.get(tableId) map { bySourceIds =>
      bySourceIds.values map {
        info => findShardById[T](info.id)
      } toSeq
    } getOrElse {
      throw new NonExistentShard("No shards for tableId: %s".format(tableId))
    }
  }

  def getRootForwardings(id: ShardId) = {
    getRootShardIds(id).map(shardManager.getForwardingForShard)
  }

  def getRootShardIds(id: ShardId): Set[ShardId] = {
    val ids = shardManager.listUpwardLinks(id)
    val set: Set[ShardId] = if (ids.isEmpty) Set(id) else Set() // type needed to avoid inferring to Collection[ShardId]
    set ++ ids.flatMap((i) => getRootShardIds(i.upId).toList)
  }

  def getCommonShardId(ids: Seq[ShardId]) = {
    ids.map(getRootShardIds).reduceLeft((s1, s2) => s1.filter(s2.contains)).toSeq.headOption
  }
}
