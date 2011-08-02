package com.twitter.gizzard.shards

import com.twitter.gizzard.nameserver.LoadBalancer


// ReplicatingShard. Forward and fail over to other children

case class ReplicatingShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends RoutingNode[T] {
  protected def loadBalancer() = new LoadBalancer(children).apply()

  protected[shards] def collectedShards(readOnly: Boolean) = {
    loadBalancer() flatMap { _.collectedShards(readOnly) }
  }
}


// Base class for all read/write flow wrapper shards

abstract class WrapperRoutingNode[T] extends RoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]): RoutingNode.Leaf[T]

  // XXX: remove when we move to shard replica sets rather than trees.
  private lazy val childrenWithPlaceholder = if (children.isEmpty) {
    Seq(LeafRoutingNode.NullNode.asInstanceOf[RoutingNode[T]])
  } else {
    children
  }

  protected[shards] def collectedShards(readOnly: Boolean) = {
    childrenWithPlaceholder flatMap {
      _.collectedShards(readOnly).map(leafTransform)
    }
  }
}


// BlockedShard. Refuse and fail all traffic.

case class BlockedShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = {
    l.copy(readBehavior = RoutingNode.Deny, writeBehavior = RoutingNode.Deny)
  }
}


// BlackHoleShard. Silently refuse all traffic.

case class BlackHoleShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = {
    l.copy(readBehavior = RoutingNode.Ignore, writeBehavior = RoutingNode.Ignore)
  }
}


// WriteOnlyShard. Fail all read traffic.

case class WriteOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = {
    l.copy(readBehavior = RoutingNode.Deny)
  }
}


// ReadOnlyShard. Fail all write traffic.

case class ReadOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = {
    l.copy(writeBehavior = RoutingNode.Deny)
  }
}


// SlaveShard. Silently refuse all write traffic.

case class SlaveShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = {
    l.copy(writeBehavior = RoutingNode.Ignore)
  }
}
