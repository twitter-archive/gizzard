package com.twitter.gizzard.nameserver

import java.util.TreeMap
import scala.collection.mutable
import com.twitter.util.Time
import com.twitter.conversions.time._
import com.twitter.querulous.StatsCollector
import com.twitter.querulous.evaluator.QueryEvaluatorFactory
import com.twitter.logging.Logger
import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler.{CopyJob, RepairJob}


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

class NameServer(
  nameServerShard: RoutingNode[com.twitter.gizzard.nameserver.Shard],
  val shardRepository: ShardRepository,
  jobRelayFactory: JobRelayFactory,
  val mappingFunction: Long => Long) {

  private val log = Logger.get(getClass.getName)

  @volatile protected var shardInfos = mutable.Map.empty[ShardId, ShardInfo]
  @volatile private var familyTree: scala.collection.Map[ShardId, Seq[LinkInfo]] = null
  @volatile private var forwardings: scala.collection.Map[Int, TreeMap[Long, ShardInfo]] = null
  @volatile var jobRelay: JobRelay = NullJobRelay


  val forwarders = mutable.Seq[Forwarder[Any]]()

  def newForwarder[T](configBuilder: ForwarderBuilder[T] => Unit) = {
    val builder = new ForwarderBuilder[T](this, shardRepository)
    configBuilder(builder)
    val manager = builder.build()
    forwarders :+ manager
    manager
  }

  def newCopyJob(from: ShardId, to: ShardId): CopyJob[Any] = {
    val manager = forwarderForShardIds(Seq(from, to))
    manager.newCopyJob(from, to)
  }

  def newRepairJob(ids: Seq[ShardId]): RepairJob[Any] = {
    val manager = forwarderForShardIds(ids)
    manager.newRepairJob(ids)
  }

  def newDiffJob(ids: Seq[ShardId]): RepairJob[Any] = {
    val manager = forwarderForShardIds(ids)
    manager.newDiffJob(ids)
  }

  private def forwarderForShardIds(ids: Seq[ShardId]) = {
    val manager = forwarders find { _ containsShard ids.head } getOrElse {
      throw new InvalidShard("Error in server configuration. Invalid shard found!")
    }

    if (ids forall { manager containsShard _ }) {
      manager
    } else {
      throw new InvalidShard("Incompatible shard types found!")
    }
  }



  @throws(classOf[ShardException])
  def createShard(shardInfo: ShardInfo) {
    createShard(shardInfo, shardRepository)
  }

  def getShardInfo(id: ShardId) = shardInfos(id)

  def getChildren(id: ShardId) = {
    if(familyTree == null) throw new NameserverUninitialized
    familyTree.getOrElse(id, new mutable.ArrayBuffer[LinkInfo])
  }

  def dumpStructure(tableIds: Seq[Int]) = nameServerShard.read.any(_.dumpStructure(tableIds))

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

    listRemoteHosts.foreach { h =>
      newRemoteClusters += h.cluster -> (h :: newRemoteClusters.getOrElse(h.cluster, Nil))
    }

    jobRelay  = jobRelayFactory(newRemoteClusters.toMap)

    recreateInternalShardState()
    log.info("Loading name server configuration is done.")
  }

  def findShardById[T](id: ShardId, weight: Int): RoutingNode[T] = {
    val (shardInfo, downwardLinks) = shardInfos.get(id).map { info =>
      // either pull shard and links from our internal data structures...
      (info, getChildren(id))
    } getOrElse {
      // or directly from the db, in the case they are not attached to a forwarding.
      (getShard(id), listDownwardLinks(id))
    }

    val children = downwardLinks.map(l => findShardById(l.downId, l.weight)).toList

    // XXX: cast!
    shardRepository.find(shardInfo, weight, children).asInstanceOf[RoutingNode[T]]
  }

  @throws(classOf[NonExistentShard])
  def findShardById[T](id: ShardId): RoutingNode[T] = findShardById(id, 1)

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

    findShardById(shardInfo.id)
  }

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
    getRootShardIds(id).map(getForwardingForShard)
  }

  @throws(classOf[ShardException])
  def getRootShardIds(id: ShardId): Set[ShardId] = {
    val ids = listUpwardLinks(id)
    val set: Set[ShardId] = if (ids.isEmpty) Set(id) else Set() // type needed to avoid inferring to Collection[ShardId]
    set ++ ids.flatMap((i) => getRootShardIds(i.upId).toList)
  }

  def getCommonShardId(ids: Seq[ShardId]) = {
    ids.map(getRootShardIds).reduceLeft((s1, s2) => s1.filter(s2.contains)).toSeq.headOption
  }

  @throws(classOf[ShardException])
  def createShard(shardInfo: ShardInfo, repository: ShardRepository) {
    nameServerShard.write.foreach(_.createShard(shardInfo, repository))
  }

  @throws(classOf[ShardException])
  def getShard(id: ShardId) = {
    nameServerShard.read.any(_.getShard(id))
  }

  @throws(classOf[ShardException])
  def deleteShard(id: ShardId) {
    nameServerShard.write.foreach(_.deleteShard(id))
  }

  @throws(classOf[ShardException])
  def addLink(upId: ShardId, downId: ShardId, weight: Int) {
    nameServerShard.write.foreach(_.addLink(upId, downId, weight))
  }

  @throws(classOf[ShardException])
  def removeLink(upId: ShardId, downId: ShardId) {
    nameServerShard.write.foreach(_.removeLink(upId, downId))
  }

  @throws(classOf[ShardException])
  def listUpwardLinks(id: ShardId) = {
    nameServerShard.read.any(_.listUpwardLinks(id))
  }

  @throws(classOf[ShardException])
  def listDownwardLinks(id: ShardId) = {
    nameServerShard.read.any(_.listDownwardLinks(id))
  }

  @throws(classOf[ShardException])
  def listLinks() = {
    nameServerShard.read.any(_.listLinks())
  }

  @throws(classOf[ShardException])
  def markShardBusy(id: ShardId, busy: Busy.Value) {
    nameServerShard.write.foreach(_.markShardBusy(id, busy))
  }

  @throws(classOf[ShardException])
  def setForwarding(forwarding: Forwarding) {
    nameServerShard.write.foreach(_.setForwarding(forwarding))
  }

  @throws(classOf[ShardException])
  def replaceForwarding(oldId: ShardId, newId: ShardId) {
    nameServerShard.write.foreach(_.replaceForwarding(oldId, newId))
  }

  @throws(classOf[ShardException])
  def getForwarding(tableId: Int, baseId: Long) = {
    nameServerShard.read.any(_.getForwarding(tableId, baseId))
  }

  @throws(classOf[ShardException])
  def getForwardingForShard(id: ShardId) = {
    nameServerShard.read.any(_.getForwardingForShard(id))
  }

  @throws(classOf[ShardException])
  def getForwardings() = {
    nameServerShard.read.any(_.getForwardings())
  }

  @throws(classOf[ShardException])
  def shardsForHostname(hostname: String) = {
    nameServerShard.read.any(_.shardsForHostname(hostname))
  }

  @throws(classOf[ShardException])
  def listShards() = {
    nameServerShard.read.any(_.listShards())
  }

  @throws(classOf[ShardException])
  def getBusyShards() = {
    nameServerShard.read.any(_.getBusyShards())
  }

  @throws(classOf[ShardException])
  def rebuildSchema() {
    nameServerShard.write.foreach(_.rebuildSchema())
  }

  @throws(classOf[ShardException])
  def removeForwarding(f: Forwarding) {
    nameServerShard.write.foreach(_.removeForwarding(f))
  }

  @throws(classOf[ShardException])
  def listHostnames() = {
    nameServerShard.read.any(_.listHostnames())
  }

  @throws(classOf[ShardException])
  def listTables() = {
    nameServerShard.read.any(_.listTables())
  }


  // Remote Host Management

  @throws(classOf[ShardException])
  def addRemoteHost(h: Host) {
    nameServerShard.write.foreach(_.addRemoteHost(h))
  }

  @throws(classOf[ShardException])
  def removeRemoteHost(h: String, p: Int) {
    nameServerShard.write.foreach(_.removeRemoteHost(h, p))
  }

  @throws(classOf[ShardException])
  def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value) {
    nameServerShard.write.foreach(_.setRemoteHostStatus(h, p, s))
  }

  @throws(classOf[ShardException])
  def setRemoteClusterStatus(c: String, s: HostStatus.Value) {
    nameServerShard.write.foreach(_.setRemoteClusterStatus(c, s))
  }


  @throws(classOf[ShardException])
  def getRemoteHost(h: String, p: Int) = {
    nameServerShard.read.any(_.getRemoteHost(h, p))
  }

  @throws(classOf[ShardException])
  def listRemoteClusters() = {
    nameServerShard.read.any(_.listRemoteClusters())
  }

  @throws(classOf[ShardException])
  def listRemoteHosts() = {
    nameServerShard.read.any(_.listRemoteHosts())
  }

  @throws(classOf[ShardException])
  def listRemoteHostsInCluster(c: String) = {
    nameServerShard.read.any(_.listRemoteHostsInCluster(c))
  }
}
