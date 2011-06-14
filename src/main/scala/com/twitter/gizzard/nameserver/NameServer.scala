package com.twitter.gizzard.nameserver

import java.util.TreeMap
import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.logging.Logger
import com.twitter.gizzard.shards._


class NonExistentShard(message: String) extends ShardException(message: String)
class InvalidShard(message: String) extends ShardException(message: String)
class NameserverUninitialized extends ShardException("Please call reload() before operating on the NameServer")

object TreeUtils {
  protected[nameserver] def mapOfSets[A,B](s: Iterable[A])(getKey: A => B): Map[B,Set[A]] = {
    s.foldLeft(Map[B,Set[A]]()) { (m, item) =>
      val key = getKey(item)
      m + (key -> m.get(key).map(_ + item).getOrElse(Set(item)))
    }
  }

  protected[nameserver] def collectFromTree[A,B](roots: Iterable[A])(lookup: A => Iterable[B])(nextKey: B => A): List[B] = {

    // if lookup is a map, just rescue and return an empty list for flatMap
    def getOrElse(a: A) = try { lookup(a) } catch { case e: NoSuchElementException => Nil }

    if (roots.isEmpty) Nil else {
      val elems = roots.flatMap(getOrElse).toList
      elems ++ collectFromTree(elems.map(nextKey))(lookup)(nextKey)
    }
  }

  protected[nameserver] def descendantLinks(ids: Set[ShardId])(f: ShardId => Iterable[LinkInfo]): Set[LinkInfo] = {
    collectFromTree(ids)(f)(_.downId).toSet
  }
}

class ShardManager(shard: RoutingNode[com.twitter.gizzard.nameserver.Shard]) {
  def dumpStructure(tableIds: Seq[Int]) = shard.read.any(_.dumpStructure(tableIds))

  def createShard(shardInfo: ShardInfo)            { shard.write.foreach(_.createShard(shardInfo)) }
  def deleteShard(id: ShardId)                     { shard.write.foreach(_.deleteShard(id)) }
  def markShardBusy(id: ShardId, busy: Busy.Value) { shard.write.foreach(_.markShardBusy(id, busy)) }

  // XXX: removing this causes CopyJobSpec to fail.
  @throws(classOf[NonExistentShard])
  def getShard(id: ShardId)               = shard.read.any(_.getShard(id))
  def shardsForHostname(hostname: String) = shard.read.any(_.shardsForHostname(hostname))
  def listShards()                        = shard.read.any(_.listShards())
  def getBusyShards()                     = shard.read.any(_.getBusyShards())


  def addLink(upId: ShardId, downId: ShardId, weight: Int) { shard.write.foreach(_.addLink(upId, downId, weight)) }
  def removeLink(upId: ShardId, downId: ShardId)           { shard.write.foreach(_.removeLink(upId, downId)) }

  def listUpwardLinks(id: ShardId)   = shard.read.any(_.listUpwardLinks(id))
  def listDownwardLinks(id: ShardId) = shard.read.any(_.listDownwardLinks(id))
  def listLinks()                    = shard.read.any(_.listLinks())


  def setForwarding(forwarding: Forwarding)             { shard.write.foreach(_.setForwarding(forwarding)) }
  def removeForwarding(f: Forwarding)                   { shard.write.foreach(_.removeForwarding(f)) }
  def replaceForwarding(oldId: ShardId, newId: ShardId) { shard.write.foreach(_.replaceForwarding(oldId, newId)) }

  def getForwarding(tableId: Int, baseId: Long) = shard.read.any(_.getForwarding(tableId, baseId))
  def getForwardingForShard(id: ShardId)        = shard.read.any(_.getForwardingForShard(id))
  def getForwardings()                          = shard.read.any(_.getForwardings())


  def listHostnames() = shard.read.any(_.listHostnames())
  def listTables()    = shard.read.any(_.listTables())
}

class RemoteClusterManager(shard: RoutingNode[com.twitter.gizzard.nameserver.Shard]) {
  def addRemoteHost(h: Host)                                      { shard.write.foreach(_.addRemoteHost(h)) }
  def removeRemoteHost(h: String, p: Int)                         { shard.write.foreach(_.removeRemoteHost(h, p)) }
  def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value) { shard.write.foreach(_.setRemoteHostStatus(h, p, s)) }
  def setRemoteClusterStatus(c: String, s: HostStatus.Value)      { shard.write.foreach(_.setRemoteClusterStatus(c, s)) }

  def getRemoteHost(h: String, p: Int)    = shard.read.any(_.getRemoteHost(h, p))
  def listRemoteClusters()                = shard.read.any(_.listRemoteClusters())
  def listRemoteHosts()                   = shard.read.any(_.listRemoteHosts())
  def listRemoteHostsInCluster(c: String) = shard.read.any(_.listRemoteHostsInCluster(c))
}

class NameServer(
  nameServerShard: RoutingNode[com.twitter.gizzard.nameserver.Shard],
  jobRelayFactory: JobRelayFactory,
  val mappingFunction: Long => Long) {

  val shardRepository = new ShardRepository

  // XXX: inject these in later
  val shardManager         = new ShardManager(nameServerShard)
  val remoteClusterManager = new RemoteClusterManager(nameServerShard)

  private val log = Logger.get(getClass.getName)

  @volatile protected var shardInfos = mutable.Map.empty[ShardId, ShardInfo]
  @volatile private var familyTree: scala.collection.Map[ShardId, Seq[LinkInfo]] = null
  @volatile private var forwardings: scala.collection.Map[Int, TreeMap[Long, ShardInfo]] = null
  @volatile var jobRelay: JobRelay = NullJobRelay

  private val singleForwarders = mutable.Map[String, SingleForwarder[_]]()
  private val multiForwarders  = mutable.Map[String, MultiForwarder[_]]()

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

  private def currentState() = nameServerShard.read.any(_.currentState())

  private def recreateInternalShardState() {
    val newShardInfos     = mutable.Map[ShardId, ShardInfo]()
    val newFamilyTree     = mutable.Map[ShardId, mutable.ArrayBuffer[LinkInfo]]()
    val newForwardings    = mutable.Map[Int, TreeMap[Long, ShardInfo]]()

    currentState().foreach { state =>

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
    nameServerShard.write.foreach(_.reload())

    val newRemoteClusters = mutable.Map[String, List[Host]]()

    remoteClusterManager.listRemoteHosts.foreach { h =>
      newRemoteClusters += h.cluster -> (h :: newRemoteClusters.getOrElse(h.cluster, Nil))
    }

    jobRelay  = jobRelayFactory(newRemoteClusters.toMap)

    recreateInternalShardState()
    log.info("Loading name server configuration is done.")
  }

  @throws(classOf[ShardException])
  def rebuildSchema() {
    nameServerShard.write.foreach(_.rebuildSchema())
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

  @throws(classOf[ShardException])
  def getRootForwardings(id: ShardId) = {
    getRootShardIds(id).map(shardManager.getForwardingForShard)
  }

  @throws(classOf[ShardException])
  def getRootShardIds(id: ShardId): Set[ShardId] = {
    val ids = shardManager.listUpwardLinks(id)
    val set: Set[ShardId] = if (ids.isEmpty) Set(id) else Set() // type needed to avoid inferring to Collection[ShardId]
    set ++ ids.flatMap((i) => getRootShardIds(i.upId).toList)
  }

  def getCommonShardId(ids: Seq[ShardId]) = {
    ids.map(getRootShardIds).reduceLeft((s1, s2) => s1.filter(s2.contains)).toSeq.headOption
  }
}
