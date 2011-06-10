package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler._


class InvalidTableId(id: Int) extends NoSuchElementException("Forwarder does not contain table "+ id)

object Forwarder {
  def canonicalNameForManifest[T](manifest: Manifest[T]) = {
    manifest.erasure.getName.split("\\.").last
  }
}

abstract class Forwarder[T](
  copyFactory: CopyJobFactory[T],
  repairFactory: RepairJobFactory[T],
  diffFactory: RepairJobFactory[T] // XXX: Why is this the same class as repair??? This should not be allowed by types.
) {

  protected def nameServer: NameServer
  def interfaceName: String
  def isValidTableId(id: Int): Boolean
  def shardFactories: Map[String, ShardFactory[T]]

  val shardTypes = shardFactories.keySet

  def isValidShardType(name: String) = {
    shardTypes contains name
  }

  def containsShard(id: ShardId): Boolean = {
    containsShard(nameServer.getShardInfo(id))
  }

  def containsRoutingNode(node: RoutingNode[_]) = {
    node.shardInfos forall { shardTypes contains _.className }
  }

  def containsShard(info: ShardInfo): Boolean = {
    isValidShardType(info.className) &&
    (nameServer.getRootForwardings(info.id) map { _.tableId } exists isValidTableId)
  }

  def findShardById(id: ShardId): Option[RoutingNode[T]] = {
    try {
      val node = nameServer.findShardById[T](id)
      if (containsRoutingNode(node)) Some(node) else None
    } catch {
      case e: NonExistentShard       => None
      case e: NoSuchElementException => None
    }
  }

  // XXX: copy, repair and diff live here for now, but it's a bit
  // jank. clean up the admin job situation.
  def newCopyJob(from: ShardId, to: ShardId) = copyFactory(from, to)
  def newRepairJob(ids: Seq[ShardId])        = repairFactory(ids)
  def newDiffJob(ids: Seq[ShardId])          = diffFactory(ids)
}

class MultiTableForwarder[T](
  val interfaceName: String,
  tableIdValidator: Int => Boolean,
  val nameServer: NameServer,
  val shardFactories: Map[String, ShardFactory[T]],
  copier: CopyJobFactory[T],
  repairer: RepairJobFactory[T],
  differ: RepairJobFactory[T])
extends Forwarder[T](copier, repairer, differ)
with PartialFunction[Int, SingleTableForwarder[T]] {

  def isValidTableId(id: Int) = tableIdValidator(id)

  def table(tableId: Int) = if (isDefinedAt(tableId)) {
    new SingleTableForwarder(interfaceName, tableId, nameServer, shardFactories, copier, repairer, differ)
  } else {
    throw new InvalidTableId(tableId)
  }

  def find(tableId: Int, baseId: Int) = {
    table(tableId).find(baseId)
  }

  def findAll(tableId: Int) = if (isDefinedAt(tableId)) {
    nameServer.findForwardings[T](tableId)
  } else {
    throw new InvalidTableId(tableId)
  }

  // satisfy PartialFunction

  def apply(id: Int) = table(id)

  def isDefinedAt(id: Int) = isValidTableId(id)
}

class SingleTableForwarder[T](
  val interfaceName: String,
  val tableId: Int,
  val nameServer: NameServer,
  val shardFactories: Map[String, ShardFactory[T]],
  copier: CopyJobFactory[T],
  repairer: RepairJobFactory[T],
  differ: RepairJobFactory[T])
extends Forwarder[T](copier, repairer, differ)
with PartialFunction[Long, RoutingNode[T]] {

  def isValidTableId(id: Int) = id == tableId

  def find(baseId: Long) = nameServer.findCurrentForwarding[T](tableId, baseId)

  def findOption(baseId: Long) = {
    try {
      Some(nameServer.findCurrentForwarding[T](tableId, baseId))
    } catch {
      case e: NonExistentShard => None
    }
  }

  // satisfy PartialFunction

  def apply(baseId: Long) = find(baseId)

  def isDefinedAt(baseId: Long) = try {
    find(baseId)
    true
  } catch {
    case e: NonExistentShard => false
  }
}

object ForwarderBuilder {
  trait Yes
  trait No

  def singleTable[T : Manifest](ns: NameServer) = new SingleTableForwarderBuilder[T, No, No](ns)
  def multiTable[T : Manifest](ns: NameServer)  = new MultiTableForwarderBuilder[T, Yes, No](ns)
}

import ForwarderBuilder._

abstract class ForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory, This[T1 >: T, A, B] <: ForwarderBuilder[T1, A, B, This]] {
  self: This[T, HasTableIds, HasShardFactory] =>

  type CurrentConfiguration = This[T, HasTableIds, HasShardFactory]
  type TablesConfigured     = This[T, Yes,         HasShardFactory]
  type ShardsConfigured     = This[T, HasTableIds, Yes]
  type FullyConfigured      = This[T, Yes,         Yes]

  protected val manifest      = implicitly[Manifest[T]]
  protected val interfaceName = Forwarder.canonicalNameForManifest(manifest)

  protected var _shardFactories: Map[String, ShardFactory[T]] = Map.empty
  protected var _copyFactory: CopyJobFactory[T]               = new NullCopyJobFactory("Copies not supported!")
  protected var _repairFactory: RepairJobFactory[T]           = new NullRepairJobFactory("Shard repair not supported!")
  protected var _diffFactory: RepairJobFactory[T]             = new NullRepairJobFactory("Shard diff not supported!")

  def copyFactory(factory: CopyJobFactory[T]) = {
    _copyFactory = factory
    this
  }

  def repairFactory(factory: RepairJobFactory[T]) = {
    _repairFactory = factory
    this
  }

  def diffFactory(factory: RepairJobFactory[T]) = {
    _diffFactory = factory
    this
  }

  def shardFactory(factory: ShardFactory[T]) = {
    _shardFactories = Map(interfaceName -> factory)
    this.asInstanceOf[ShardsConfigured]
  }

  def shardFactories(factories: (String, ShardFactory[T])*) = {
    _shardFactories = factories.toMap
    this.asInstanceOf[ShardsConfigured]
  }
}

class SingleTableForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory](ns: NameServer)
extends ForwarderBuilder[T, HasTableIds, HasShardFactory, SingleTableForwarderBuilder] {

  protected var _tableId = 0

  def tableId(id: Int) = {
    _tableId = id
    this.asInstanceOf[TablesConfigured]
  }

  def build()(implicit canBuild: CurrentConfiguration => FullyConfigured) = {
    new SingleTableForwarder[T](
      interfaceName,
      _tableId,
      ns,
      _shardFactories,
      _copyFactory,
      _repairFactory,
      _diffFactory
    )
  }
}

class MultiTableForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory](ns: NameServer)
extends ForwarderBuilder[T, HasTableIds, HasShardFactory, MultiTableForwarderBuilder] {

  protected var _tableIdValidator: Int => Boolean = { x: Int => true }

  def tableIds(ids: Set[Int]) = {
    _tableIdValidator = ids.contains
    this.asInstanceOf[TablesConfigured]
  }

  def build()(implicit canBuild: CurrentConfiguration => ShardsConfigured) = {
    new MultiTableForwarder[T](
      interfaceName,
      _tableIdValidator,
      ns,
      _shardFactories,
      _copyFactory,
      _repairFactory,
      _diffFactory
    )
  }
}
