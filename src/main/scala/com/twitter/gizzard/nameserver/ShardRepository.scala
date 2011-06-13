package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler.{CopyJob, RepairJob}


class ExistingShardFactory(name: String) extends ShardException("ShardFactory already registered for type '"+ name +"'.")
class ExistingForwarder(name: String) extends ShardException("Forwarder already registered for interface '"+ name +"'.")

class ForwarderRepository {
  private val shardRepository  = new BasicShardRepository
  private val allForwarders    = mutable.Map[String, GenericForwarder]()
  private val singleForwarders = mutable.Map[String, SingleForwarder[_]]()
  private val multiForwarders  = mutable.Map[String, MultiForwarder[_]]()

  def singleTableForwarder[T : Manifest] = {
    singleForwarders(Forwarder.nameForInterface[T]).asInstanceOf[SingleForwarder[T]]
  }

  def multiTableForwarder[T : Manifest] = {
    multiForwarders(Forwarder.nameForInterface[T]).asInstanceOf[MultiForwarder[T]]
  }

  def addSingleTableForwarder[T : Manifest](f: SingleForwarder[T]) {
    registerForwarder(f)
    singleForwarders(Forwarder.nameForInterface[T]) = f
  }

  def addMultiTableForwarder[T : Manifest](f: MultiForwarder[T]) {
    registerForwarder(f)
    multiForwarders(Forwarder.nameForInterface[T]) = f
  }

  def commonForwarderForShards(ids: Seq[ShardId]) = {
    allForwarders.values find { _ containsShard ids.head } flatMap { manager =>
      if (ids forall { manager containsShard _ }) Some(manager) else None
    } getOrElse(throw new InvalidShard("Shard ids: "+ ids.mkString(", ") +" refer to shards of incompatible types."))
  }

  // XXX: weird methods. better encapsulation needed

  def materializeShard(info: ShardInfo) {
    shardRepository.create(info)
  }

  def instantiateNode[T](info: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    shardRepository.find[T](info, weight, children)
  }

  // XXX: Do these copy job related methods belong here?

  def newCopyJob(from: ShardId, to: ShardId) = {
    commonForwarderForShards(Seq(from, to)).newCopyJob(from, to).asInstanceOf[CopyJob[Any]]
  }

  def newRepairJob(ids: Seq[ShardId]) = {
    commonForwarderForShards(ids).newRepairJob(ids).asInstanceOf[RepairJob[Any]]
  }

  def newDiffJob(ids: Seq[ShardId]) = {
    commonForwarderForShards(ids).newDiffJob(ids).asInstanceOf[RepairJob[Any]]
  }

  // helper methods

  private def registerForwarder[T : Manifest](f: Forwarder[T]) {
    val name = Forwarder.nameForInterface[T]
    if (allForwarders contains name) throw new ExistingForwarder(name)
    f.shardFactories foreach { shardRepository += _ }
    allForwarders(name) = f
  }
}


class ShardRepository {
  private val nodeFactories = mutable.Map[String, RoutingNodeFactory[Any]]()

  private[nameserver] def +=[T](item: (String, ShardFactory[T])) {
    val (className, shardFactory) = item

    if (nodeFactories contains className) {
      throw new ExistingShardFactory("Factory for "+className+" already defined!")
    } else {
      nodeFactories += (className -> new LeafRoutingNodeFactory(shardFactory.asInstanceOf[ShardFactory[Any]]))
    }
  }

  def addRoutingNode(className: String, factory: RoutingNodeFactory[Any]) {
    nodeFactories += (className -> factory)
  }

  def addRoutingNode(className: String, cons: (ShardInfo, Int, Seq[RoutingNode[Any]]) => RoutingNode[Any]) {
    addRoutingNode(className, new ConstructorRoutingNodeFactory(cons))
  }

  def find[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    val node = factory(shardInfo.className).instantiate(shardInfo, weight, children.asInstanceOf[Seq[RoutingNode[Any]]])
    node.asInstanceOf[RoutingNode[T]]
  }

  def create(shardInfo: ShardInfo) {
    factory(shardInfo.className).materialize(shardInfo)
  }

  def factory(className: String) = {
    nodeFactories.get(className) getOrElse {
      val message = "No such class: " + className + "\nValid classes:\n" + nodeFactories.keySet
      throw new NoSuchElementException(message)
    }
  }

  override def toString() = "ShardRepository(" + nodeFactories.toString + ")"
}

/**
 * A ShardRepository that is pre-seeded with read-only, write-only, replicating, and blocked
 * shard types.
 */
class BasicShardRepository extends ShardRepository {

  setupPackage("com.twitter.gizzard.shards")
  setupPackage("")

  def setupPackage(packageName: String) {
    val prefix = if (packageName == "") packageName else packageName + "."

    addRoutingNode(prefix + "ReadOnlyShard", ReadOnlyShard[Any] _)
    addRoutingNode(prefix + "BlockedShard", BlockedShard[Any] _)
    addRoutingNode(prefix + "WriteOnlyShard", WriteOnlyShard[Any] _)
    addRoutingNode(prefix + "BlackHoleShard", BlackHoleShard[Any] _)
    addRoutingNode(prefix + "SlaveShard", SlaveShard[Any] _)
    addRoutingNode(prefix + "ReplicatingShard", ReplicatingShard[Any] _)
  }
}
