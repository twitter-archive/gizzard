package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.gizzard.shards._
import com.twitter.gizzard.scheduler.{CopyJob, RepairJob}


class ExistingShardFactory(name: String) extends ShardException("ShardFactory already registered for type '"+ name +"'.")
class ExistingForwarder(name: String) extends ShardException("Forwarder already registered for interface '"+ name +"'.")

class ShardRepository {
  private val nodeFactories    = mutable.Map[String, RoutingNodeFactory[Any]]()
  private val allForwarders    = mutable.Map[String, GenericForwarder]()
  private val singleForwarders = mutable.Map[String, SingleForwarder[_]]()
  private val multiForwarders  = mutable.Map[String, MultiForwarder[_]]()

  setupPackage("com.twitter.gizzard.shards")
  setupPackage("")

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

  def commonForwarderForShards(infos: Seq[ShardInfo]) = {
    allForwarders.values find { _ isValidShardType infos.head.className } flatMap { manager =>
      if (infos forall { i => manager isValidShardType i.className }) Some(manager) else None
    } getOrElse(throw new InvalidShard("Shard infos: "+ infos.mkString(", ") +" refer to shards of incompatible types."))
  }

  // XXX: weird methods. better encapsulation needed

  def materializeShard(info: ShardInfo) {
    factory(info.className).materialize(info)
  }

  def instantiateNode[T](info: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    factory[T](info.className).instantiate(info, weight, children)
  }

  // XXX: Do these copy job related methods belong here?

  def newCopyJob(from: ShardInfo, to: ShardInfo) = {
    commonForwarderForShards(Seq(from, to)).newCopyJob(from.id, to.id)
  }

  def newRepairJob(infos: Seq[ShardInfo]) = {
    commonForwarderForShards(infos).newRepairJob(infos.map(_.id))
  }

  def newDiffJob(infos: Seq[ShardInfo]) = {
    commonForwarderForShards(infos).newDiffJob(infos.map(_.id))
  }

  // helper methods

  def factory[T](className: String) = {
    nodeFactories.get(className) map { _.asInstanceOf[RoutingNodeFactory[T]] } getOrElse {
      val message = "No such class: " + className + "\nValid classes:\n" + nodeFactories.keySet
      throw new NoSuchElementException(message)
    }
  }

  private def registerShardfactory[T](name: String, f: ShardFactory[T]) {
    if (nodeFactories contains name) throw new ExistingShardFactory(name)
    nodeFactories(name) = new LeafRoutingNodeFactory[Any](f)
  }

  private def registerForwarder[T : Manifest](f: Forwarder[T]) {
    val name = Forwarder.nameForInterface[T]
    if (allForwarders contains name) throw new ExistingForwarder(name)
    f.shardFactories foreach { case (n, f) => registerShardfactory(n, f) }
    allForwarders(name) = f
  }

  private def setupPackage(packageName: String) {
    val prefix = if (packageName == "") packageName else packageName + "."

    nodeFactories(prefix +"ReadOnlyShard")    = new ConstructorRoutingNodeFactory(ReadOnlyShard[Any] _)
    nodeFactories(prefix +"BlockedShard")     = new ConstructorRoutingNodeFactory(BlockedShard[Any] _)
    nodeFactories(prefix +"WriteOnlyShard")   = new ConstructorRoutingNodeFactory(WriteOnlyShard[Any] _)
    nodeFactories(prefix +"BlackHoleShard")   = new ConstructorRoutingNodeFactory(BlackHoleShard[Any] _)
    nodeFactories(prefix +"SlaveShard")       = new ConstructorRoutingNodeFactory(SlaveShard[Any] _)
    nodeFactories(prefix +"ReplicatingShard") = new ConstructorRoutingNodeFactory(ReplicatingShard[Any] _)
  }
}
