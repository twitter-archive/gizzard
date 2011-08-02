package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler._


class InvalidTableId(id: Int) extends NoSuchElementException("Forwarder does not contain table "+ id)

object Forwarder {
  def nameForInterface[T : Manifest] = nameFromManifest(implicitly[Manifest[T]])

  def nameFromManifest[T](manifest: Manifest[T]) = manifest.erasure.getName.split("\\.").last
}

object ForwarderBuilder {
  trait Yes
  trait No

  def singleTable[T : Manifest] = new SingleForwarderBuilder[T, No, No]
  def multiTable[T : Manifest]  = new MultiForwarderBuilder[T, Yes, No]
}

import ForwarderBuilder._

trait GenericForwarder {
  def interfaceName: String
  def shardTypes: Set[String]
  def isValidTableId(id: Int): Boolean
  def isValidShardType(name: String): Boolean
  def contains(node: RoutingNode[_]): Boolean

  // XXX: copy, repair and diff live here for now, but it's a bit
  // jank. clean up the admin job situation.
  def newCopyJob(from: ShardId, to: ShardId): JsonJob
  def newRepairJob(ids: Seq[ShardId]): JsonJob
  def newDiffJob(ids: Seq[ShardId]): JsonJob
}

abstract class Forwarder[T](protected val nameServer: NameServer, config: ForwarderBuilder[T, Yes, Yes])
extends GenericForwarder {

  def isValidTableId(id: Int): Boolean

  val interfaceName  = config.interfaceName
  val shardFactories = config._shardFactories
  val copyFactory    = config._copyFactory
  val repairFactory  = config._repairFactory
  val diffFactory    = config._diffFactory // XXX: Why is this the same class as repair???
                                           // This should not be allowed by types.

  val shardTypes = shardFactories.keys.toSet

  def isValidShardType(name: String): Boolean = {
    shardTypes contains name
  }

  def contains(node: RoutingNode[_]) = {
    node.shardInfos forall { shardTypes contains _.className }
  }

  def findShardById(id: ShardId): Option[RoutingNode[T]] = {
    try {
      val node = nameServer.findShardById[T](id)
      if (contains(node)) Some(node) else None
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

class SingleForwarder[T](ns: NameServer, config: SingleForwarderBuilder[T, Yes, Yes])
extends Forwarder[T](ns, config)
with PartialFunction[Long, RoutingNode[T]] {

  val tableId = config._tableId

  def isValidTableId(id: Int) = id == tableId

  def find(baseId: Long) = nameServer.findCurrentForwarding[T](tableId, baseId)

  def findOption(baseId: Long) = try {
    Some(find(baseId))
  } catch {
    case e: NonExistentShard => None
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

class MultiForwarder[T](ns: NameServer, config: MultiForwarderBuilder[T, Yes, Yes])
extends Forwarder[T](ns, config) {

  val tableIdValidator = config._tableIdValidator

  def isValidTableId(id: Int) = tableIdValidator(id)

  def find(tableId: Int, baseId: Long) = if (isValidTableId(tableId)) {
    nameServer.findCurrentForwarding[T](tableId, baseId)
  } else {
    throw new InvalidTableId(tableId)
  }

  def findOption(tableId: Int, baseId: Long) = try {
    Some(find(tableId, baseId))
  } catch {
    case e: NonExistentShard => None
  }

  def findAll(tableId: Int) = if (isValidTableId(tableId)) {
    nameServer.findForwardings[T](tableId)
  } else {
    throw new InvalidTableId(tableId)
  }
}

abstract class ForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory] {
  val manifest      = implicitly[Manifest[T]]
  val interfaceName = Forwarder.nameFromManifest(manifest)

  protected[nameserver] var _shardFactories: Map[String, ShardFactory[T]] = Map.empty
  protected[nameserver] var _copyFactory: CopyJobFactory[T]     = new NullCopyJobFactory("Copies not supported!")
  protected[nameserver] var _repairFactory: RepairJobFactory[T] = new NullRepairJobFactory("Shard repair not supported!")
  protected[nameserver] var _diffFactory: RepairJobFactory[T]   = new NullRepairJobFactory("Shard diff not supported!")
}

abstract class AbstractForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory, This[T1 >: T, A, B] <: AbstractForwarderBuilder[T1, A, B, This]]
extends ForwarderBuilder[T, HasTableIds, HasShardFactory] {
  self: This[T, HasTableIds, HasShardFactory] =>

  type CurrentConfiguration = This[T, HasTableIds, HasShardFactory]
  type TablesConfigured     = This[T, Yes,         HasShardFactory]
  type ShardsConfigured     = This[T, HasTableIds, Yes]
  type FullyConfigured      = This[T, Yes,         Yes]

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

class SingleForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory]
extends AbstractForwarderBuilder[T, HasTableIds, HasShardFactory, SingleForwarderBuilder] {

  protected[nameserver] var _tableId = 0

  def tableId(id: Int) = {
    _tableId = id
    this.asInstanceOf[TablesConfigured]
  }

  def build(ns: NameServer)(implicit canBuild: CurrentConfiguration => FullyConfigured) = {
    new SingleForwarder[T](ns, this)
  }
}

class MultiForwarderBuilder[T : Manifest, HasTableIds, HasShardFactory]
extends AbstractForwarderBuilder[T, HasTableIds, HasShardFactory, MultiForwarderBuilder] {

  protected[nameserver] var _tableIdValidator: Int => Boolean = { x: Int => true }

  def tableIds(ids: Set[Int]) = {
    _tableIdValidator = ids.contains
    this.asInstanceOf[TablesConfigured]
  }

  def build(ns: NameServer)(implicit canBuild: CurrentConfiguration => FullyConfigured) = {
    new MultiForwarder[T](ns, this)
  }
}
