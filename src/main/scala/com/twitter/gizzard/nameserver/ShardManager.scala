package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.gizzard.shards._
import com.twitter.gizzard.util.TreeUtils


class ShardManager(shard: RoutingNode[ShardManagerSource], repo: ShardRepository) {
  def reload()                          = shard.write.foreach(_.reload)
  def currentState()                    = shard.read.any(_.currentState)
  def dumpStructure(tableIds: Seq[Int]) = shard.read.any(_.dumpStructure(tableIds))

  @throws(classOf[ShardException])
  def createAndMaterializeShard(shardInfo: ShardInfo) {
    shard.write.foreach(_.createShard(shardInfo))
    repo.materializeShard(shardInfo)
  }

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

  def incrementStateVersion() { shard.write.foreach(_.incrementStateVersion()) }
  def getCurrentStateVersion() = shard.read.any(_.getCurrentStateVersion())
  def getMasterStateVersion() = shard.read.any(_.getMasterStateVersion())

  def batchExecute(commands : Seq[BatchedCommand]) { shard.write.foreach(_.batchExecute(commands)) }
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

  @throws(classOf[ShardException]) def getCurrentStateVersion() : Long
  @throws(classOf[ShardException]) def getMasterStateVersion() : Long
  @throws(classOf[ShardException]) def incrementStateVersion()

  @throws(classOf[ShardException]) def batchExecute(commands : Seq[BatchedCommand])
}
