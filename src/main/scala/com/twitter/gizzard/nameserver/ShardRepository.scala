package com.twitter.gizzard
package nameserver

import scala.collection.mutable
import shards._


class ShardRepository[T] {
  private val nodeFactories = mutable.Map[String, RoutingNodeFactory[T]]()

  def +=(item: (String, ShardFactory[T])) {
    val (className, shardFactory) = item
    nodeFactories += (className -> new LeafRoutingNodeFactory(shardFactory))
  }

  def addRoutingNode(className: String, factory: RoutingNodeFactory[T]) {
    nodeFactories += (className -> factory)
  }

  def addRoutingNode(className: String, cons: (ShardInfo, Int, Seq[RoutingNode[T]]) => RoutingNode[T]) {
    addRoutingNode(className, new ConstructorRoutingNodeFactory(cons))
  }

  def find(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    factory(shardInfo.className).instantiate(shardInfo, weight, children)
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
class BasicShardRepository[T](replicationFuture: Option[Future])
extends ShardRepository[T] {

  setupPackage("com.twitter.gizzard.shards")
  setupPackage("")

  def setupPackage(packageName: String) {
    val prefix = if (packageName == "") packageName else packageName + "."

    addRoutingNode(prefix + "ReadOnlyShard", ReadOnlyShard[T] _)
    addRoutingNode(prefix + "BlockedShard", BlockedShard[T] _)
    addRoutingNode(prefix + "WriteOnlyShard", WriteOnlyShard[T] _)
    addRoutingNode(prefix + "BlackHoleShard", BlackHoleShard[T] _)
    addRoutingNode(prefix + "ReplicatingShard", new shards.ReplicatingShardFactory[T](replicationFuture))
    addRoutingNode(prefix + "FailingOverShard", new shards.FailingOverShardFactory[T](replicationFuture))
  }
}
