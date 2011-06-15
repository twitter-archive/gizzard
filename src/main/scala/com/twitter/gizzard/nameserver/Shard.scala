package com.twitter.gizzard
package nameserver

import scala.collection.mutable
import shards._


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

trait ShardManagerSource {
  @throws(classOf[ShardException]) def reload()
  @throws(classOf[ShardException]) def currentState(): Seq[NameServerState]
  @throws(classOf[ShardException]) def dumpStructure(tableIds: Seq[Int]) = {
    import TreeUtils._

    lazy val shardsById         = listShards().map(s => s.id -> s).toMap
    lazy val linksByUpId        = mapOfSets(listLinks())(_.upId)
    lazy val forwardingsByTable = mapOfSets(getForwardingsForTableIds(tableIds))(_.tableId)

    def extractor(id: Int) = NameServerState.extractTable(id)(forwardingsByTable)(linksByUpId)(shardsById)

    tableIds.map(extractor)
  }

  @throws(classOf[ShardException]) def createShard(shardInfo: ShardInfo)
  @throws(classOf[ShardException]) def deleteShard(id: ShardId)
  @throws(classOf[ShardException]) def markShardBusy(id: ShardId, busy: Busy.Value)

  @throws(classOf[ShardException]) def getShard(id: ShardId): ShardInfo
  @throws(classOf[ShardException]) def shardsForHostname(hostname: String): Seq[ShardInfo]
  @throws(classOf[ShardException]) def listShards(): Seq[ShardInfo]
  @throws(classOf[ShardException]) def getBusyShards(): Seq[ShardInfo]


  @throws(classOf[ShardException]) def addLink(upId: ShardId, downId: ShardId, weight: Int)
  @throws(classOf[ShardException]) def removeLink(upId: ShardId, downId: ShardId)

  @throws(classOf[ShardException]) def listUpwardLinks(id: ShardId): Seq[LinkInfo]
  @throws(classOf[ShardException]) def listDownwardLinks(id: ShardId): Seq[LinkInfo]
  @throws(classOf[ShardException]) def listLinks(): Seq[LinkInfo]

  @throws(classOf[ShardException]) def setForwarding(forwarding: Forwarding)
  @throws(classOf[ShardException]) def removeForwarding(forwarding: Forwarding)
  @throws(classOf[ShardException]) def replaceForwarding(oldId: ShardId, newId: ShardId)

  @throws(classOf[ShardException]) def getForwarding(tableId: Int, baseId: Long): Forwarding
  @throws(classOf[ShardException]) def getForwardingForShard(id: ShardId): Forwarding
  @throws(classOf[ShardException]) def getForwardings(): Seq[Forwarding]
  @throws(classOf[ShardException]) def getForwardingsForTableIds(tableIds: Seq[Int]): Seq[Forwarding]

  @throws(classOf[ShardException]) def listHostnames(): Seq[String]
  @throws(classOf[ShardException]) def listTables(): Seq[Int]

}

trait RemoteClusterManagerSource {
  @throws(classOf[ShardException]) def reload()
  @throws(classOf[ShardException]) def addRemoteHost(h: Host)
  @throws(classOf[ShardException]) def removeRemoteHost(h: String, p: Int)
  @throws(classOf[ShardException]) def setRemoteHostStatus(h: String, p: Int, s: HostStatus.Value)
  @throws(classOf[ShardException]) def setRemoteClusterStatus(c: String, s: HostStatus.Value)

  @throws(classOf[ShardException]) def getRemoteHost(h: String, p: Int): Host
  @throws(classOf[ShardException]) def listRemoteClusters(): Seq[String]
  @throws(classOf[ShardException]) def listRemoteHosts(): Seq[Host]
  @throws(classOf[ShardException]) def listRemoteHostsInCluster(c: String): Seq[Host]
}
