package com.twitter.gizzard.shards


// Base class for all read/write flow wrapper shards

abstract class WrapperRoutingNode[T] extends RoutingNode[T] {

  protected def leafTransform(l: RoutingNode.Leaf[T]): RoutingNode.Leaf[T]

  protected[shards] def collectedShards(readOnly: Boolean) = children flatMap { _.collectedShards(readOnly).map(leafTransform) }
}


// BlockedShard. Refuse and fail all traffic.

case class BlockedShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = l.copy(readBehavior = RoutingNode.Deny, writeBehavior = RoutingNode.Deny)
}


// BlackHoleShard. Silently refuse all traffic.

case class BlackHoleShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = l.copy(readBehavior = RoutingNode.Ignore, writeBehavior = RoutingNode.Ignore)
}


// WriteOnlyShard. Fail all read traffic.

case class WriteOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = l.copy(readBehavior = RoutingNode.Deny)
}


// ReadOnlyShard. Fail all write traffic.

case class ReadOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = l.copy(writeBehavior = RoutingNode.Deny)
}


// SlaveShard. Silently refuse all write traffic.

case class SlaveShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {
  protected def leafTransform(l: RoutingNode.Leaf[T]) = l.copy(writeBehavior = RoutingNode.Ignore)
}
