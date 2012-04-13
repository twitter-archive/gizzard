package com.twitter.gizzard.nameserver

import java.util.TreeMap
import java.util.concurrent.{ConcurrentMap, ConcurrentHashMap}
import scala.collection.mutable
import com.twitter.logging.Logger
import com.twitter.gizzard.shards._
import com.twitter.gizzard.thrift.HostWeightInfo


class NonExistentShard(message: String) extends ShardException(message: String)
class InvalidShard(message: String) extends ShardException(message: String)
class NameserverUninitialized extends ShardException("Please call reload() before operating on the NameServer")


class RoutingState(
  instantiateNode: (ShardInfo, Weight, Seq[RoutingNode[Any]]) => RoutingNode[Any],
  infos: Iterable[ShardInfo],
  links: Iterable[LinkInfo],
  hostWeights: Iterable[HostWeightInfo],
  forwardings: Iterable[Forwarding]
) {

  val infoMap = (infos foldLeft mutable.Map[ShardId, ShardInfo]()) { (m, info) => m(info.id) = info; m }

  val linkMap = (links foldLeft mutable.Map[ShardId, List[(ShardId, Int)]]()) { (m, link) =>
    m(link.upId) = (Pair(link.downId, link.weight) :: m.getOrElse(link.upId, Nil))
    m
  }

  val hostWeightMap = hostWeights.map{ hw => hw.hostname -> hw }.toMap

  private def constructRoutingNode(root: ShardId, rawWeight: Int): RoutingNode[Any] = {
    val weight = Weight(rawWeight, hostWeightMap.get(root.hostname))
    infoMap.get(root) map { rootInfo =>
      val children =
        linkMap.getOrElse(root, Nil).map { case (id, wt) => constructRoutingNode(id, wt) }
      instantiateNode(rootInfo, weight, children)
    } getOrElse {
      throw new InvalidShard("Expected shard '"+ root +"' to exist, but it wasn't found.")
    }
  }

  def buildForwardingTree(): ConcurrentMap[Int, TreeMap[Long, RoutingNode[Any]]] = {
    val rv = new ConcurrentHashMap[Int, TreeMap[Long, RoutingNode[Any]]]()

    forwardings foreach { case Forwarding(tableId, baseId, rootShardId) =>
      rv.putIfAbsent(tableId, new TreeMap[Long, RoutingNode[Any]])
      val tree = rv.get(tableId)
      tree.put(baseId, constructRoutingNode(rootShardId, 1))
    }

    rv
  }
}

class NameServer(val shard: RoutingNode[ShardManagerSource], val mappingFunction: Long => Long) {

  import ForwarderBuilder._

  private val log = Logger.get(getClass.getName)

  // XXX: inject these in later
  val shardRepository = new ShardRepository
  val shardManager    = new ShardManager(shard, shardRepository)

  @volatile private var forwardingTree: ConcurrentMap[Int, TreeMap[Long, RoutingNode[Any]]] = _
  @volatile private var lastUpdatedSeq: Long = -1L

  // Forwarders

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

  def reload() {
    log.info("Loading name server configuration...")
    synchronized {
      shardManager.prepareReload()

      val infos       = mutable.ArrayBuffer[ShardInfo]()
      val links       = mutable.ArrayBuffer[LinkInfo]()
      val hostWeights = mutable.ArrayBuffer[HostWeightInfo]()
      val forwardings = mutable.ArrayBuffer[Forwarding]()
      val (states, updatedSeq) = shardManager.currentState()

      states foreach { state =>
        infos       ++= state.shards
        links       ++= state.links
        hostWeights ++= state.hostWeights
        forwardings ++= state.forwardings
      }

      val routes = new RoutingState(
        shardRepository.instantiateNode,
        infos,
        links,
        hostWeights,
        forwardings
      )

      forwardingTree = routes.buildForwardingTree()
      lastUpdatedSeq = updatedSeq
    }
    log.info("Loading name server configuration is done.")
  }

  /**
   * Constructs a RoutingNode via recursive trips to the ShardManager database.
   * TODO(hyungoo): is this equivalent to findShardById[Any]?
   */
  private def fetchRoutingNode(shardId: ShardId, rawWeight: Int): RoutingNode[Any] = {
    val shardInfo = shardManager.getShard(shardId)
    val weight = Weight(rawWeight, shardManager.getHostWeight(shardId.hostname))
    val children =
      shardManager.listDownwardLinks(shardId).map { link =>
        fetchRoutingNode(link.downId, link.weight)
      }
    shardRepository.instantiateNode(shardInfo, weight, children)
  }

  def reloadUpdatedForwardings() {
    log.info("Loading updated name server configuration...")
    synchronized {
      if (forwardingTree == null) throw new NameserverUninitialized

      val changes: NameServerChanges = shardManager.diffState(lastUpdatedSeq)
      val updatedForwardingsByTableId = changes.updatedForwardings.groupBy(_.tableId)
      val deletedForwardingsByTableId = changes.deletedForwardings.groupBy(_.tableId)
      val tableIds = updatedForwardingsByTableId.keySet ++ deletedForwardingsByTableId.keySet

      tableIds foreach { tableId =>
        val newTreeMap = forwardingTree.get(tableId) match {
          case null => new TreeMap[Long, RoutingNode[Any]]()
          case treeMap => new TreeMap[Long, RoutingNode[Any]](treeMap)  // create a shallow copy
        }

        deletedForwardingsByTableId.get(tableId).getOrElse(Nil) foreach { f => newTreeMap.remove(f.baseId) }
        updatedForwardingsByTableId.get(tableId).getOrElse(Nil) foreach { f =>
          newTreeMap.put(f.baseId, fetchRoutingNode(f.shardId, 1))
        }

        forwardingTree.put(tableId, newTreeMap)
      }

      lastUpdatedSeq = changes.updatedSeq
    }
    log.info("Loading updated name server configuration is done.")
  }

  // XXX: removing this causes CopyJobSpec to fail.
  // This method now always falls back to the db.
  @throws(classOf[NonExistentShard])
  def findShardById[T](id: ShardId, rawWeight: Int): RoutingNode[T] = {
    val shardInfo     = shardManager.getShard(id)
    val weight        = Weight(rawWeight, shardManager.getHostWeight(id.hostname))
    val downwardLinks = shardManager.listDownwardLinks(id)
    val children      = downwardLinks.map(l => findShardById[T](l.downId, l.weight)).toList

    shardRepository.instantiateNode[T](shardInfo, weight, children)
  }

  // XXX: removing this causes CopyJobSpec to fail.
  // This method now always falls back to the db.
  @throws(classOf[NonExistentShard])
  def findShardById[T](id: ShardId): RoutingNode[T] = findShardById(id, 1)

  @throws(classOf[NonExistentShard])
  def findCurrentForwarding[T](tableId: Int, id: Long): RoutingNode[T] = {
    if (forwardingTree == null) throw new NameserverUninitialized

    val rv = Option(forwardingTree.get(tableId)) flatMap { treeMap =>
      treeMap.floorEntry(mappingFunction(id)) match {
        case null => None
        case item => Some(item.getValue)
      }
    } getOrElse {
      throw new NonExistentShard("No shard for address: %s %s".format(tableId, id))
    }

    // XXX: cast!
    rv.asInstanceOf[RoutingNode[T]]
  }

  @throws(classOf[NonExistentShard])
  def findForwardings[T](tableId: Int): Seq[RoutingNode[T]] = {
    import scala.collection.JavaConversions._

    if (forwardingTree == null) throw new NameserverUninitialized

    val rv = Option(forwardingTree.get(tableId)) map { treeMap =>
      treeMap.values.toSeq
    } getOrElse {
      throw new NonExistentShard("No shards for tableId: %s".format(tableId))
    }

    // XXX: cast!
    rv.asInstanceOf[Seq[RoutingNode[T]]]
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
