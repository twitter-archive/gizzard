package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.gizzard.shards._
import com.twitter.gizzard.util.TreeUtils
import com.twitter.gizzard.thrift


class ShardManager(shard: RoutingNode[ShardManagerSource], repo: ShardRepository) {
  def prepareReload()                   = shard.write.foreach(_.prepareReload)
  def currentState()                    = shard.read.any(_.currentState)
  def diffState(lastUpdatedSeq: Long)   = shard.read.any(_.diffState(lastUpdatedSeq))
  def dumpStructure(tableIds: Seq[Int]) = shard.read.any(_.dumpStructure(tableIds))

  def batchExecute(commands : Seq[TransformOperation]) { shard.write.foreach(_.batchExecute(commands)) }

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
  def getHostWeight(hostname: String): Option[thrift.HostWeightInfo] = shard.read.any(_.getHostWeight(hostname))

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

trait ShardManagerSource {
  @throws(classOf[ShardException]) def prepareReload()
  @throws(classOf[ShardException]) def currentState(): (Seq[NameServerState], Long)
  @throws(classOf[ShardException]) def diffState(lastUpdatedSeq: Long): NameServerChanges
  @throws(classOf[ShardException]) def dumpStructure(tableIds: Seq[Int]) = {
    import TreeUtils._

    lazy val shardsById         = listShards().map(s => s.id -> s).toMap
    lazy val hostWeights        = listHostWeights()
    lazy val linksByUpId        = mapOfSets(listLinks())(_.upId)
    lazy val forwardingsByTable = mapOfSets(getForwardingsForTableIds(tableIds))(_.tableId)

    tableIds.map { id =>
      NameServerState.extractTable(
        id,
        forwardingsByTable,
        hostWeights,
        linksByUpId,
        shardsById
      )
    }
  }

  @throws(classOf[ShardException]) def batchExecute(commands : Seq[TransformOperation])

  @throws(classOf[ShardException]) def createShard(shardInfo: ShardInfo)
  @throws(classOf[ShardException]) def deleteShard(id: ShardId)
  @throws(classOf[ShardException]) def markShardBusy(id: ShardId, busy: Busy.Value)

  @throws(classOf[ShardException]) def getShard(id: ShardId): ShardInfo
  @throws(classOf[ShardException]) def shardsForHostname(hostname: String): Seq[ShardInfo]
  @throws(classOf[ShardException]) def listShards(): Seq[ShardInfo]
  @throws(classOf[ShardException]) def getBusyShards(): Seq[ShardInfo]
  @throws(classOf[ShardException]) def getHostWeight(hostname: String): Option[thrift.HostWeightInfo]
  @throws(classOf[ShardException]) def listHostWeights(): Seq[thrift.HostWeightInfo]


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

  @throws(classOf[ShardException]) def logCreate(id: Array[Byte], logName: String): Unit
  @throws(classOf[ShardException]) def logGet(logName: String): Option[Array[Byte]]
  @throws(classOf[ShardException]) def logEntryPush(logId: Array[Byte], entry: thrift.LogEntry): Unit
  @throws(classOf[ShardException]) def logEntryPeek(logId: Array[Byte], count: Int): Seq[thrift.LogEntry]
  @throws(classOf[ShardException]) def logEntryPop(logId: Array[Byte], entryId: Int): Unit

  /** For JMocker. TODO: switch to a better mocking framework */
  override def toString() = "<%s>".format(this.getClass)
}
