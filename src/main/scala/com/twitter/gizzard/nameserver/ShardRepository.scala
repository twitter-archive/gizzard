package com.twitter.gizzard.nameserver

import scala.collection.mutable
import com.twitter.gizzard.shards._


class ExistingShardFactory(message: String) extends ShardException(message)

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
    factory(shardInfo.className).instantiate(shardInfo, weight, children.asInstanceOf[Seq[RoutingNode[Any]]])
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
