package com.twitter.gizzard.shards

import com.twitter.gizzard.nameserver.LoadBalancer
import RoutingNode._


// ReplicatingShard. Forward and fail over to other children

case class ReplicatingShard[T](shardInfo: ShardInfo, weight: Weight, children: Seq[RoutingNode[T]])
extends RoutingNode[T] {
  protected[shards] val loadBalancer: LoadBalancer = LoadBalancer.WeightedRandom
  protected[shards] def collectedShards(readOnly: Boolean, rb: Behavior, wb: Behavior) =
    if (readOnly) {
      val (ordered, denied) = loadBalancer.balanced(children)
      val denyReadBehavior = Behavior.Ordering.max(Deny, rb)
      // rather than filtering nodes that shouldn't receive reads, we Deny them via Behavior
      ordered.flatMap(_.collectedShards(readOnly, rb, wb)) ++
        denied.flatMap(_.collectedShards(readOnly, denyReadBehavior, wb))
    } else {
      // TODO: write weights should eventually allow for fractional blocking of shards by
      // 'Deny'ing a random fraction of writes matching the write weight
      children.flatMap(_.collectedShards(readOnly, rb, wb))
    }
}


// Base class for all read/write flow wrapper shards

abstract class WrapperRoutingNode[T] extends RoutingNode[T] {
  // read and write behavior to be merged with the behavior of children
  protected def readBehavior: Behavior
  protected def writeBehavior: Behavior

  // XXX: remove when we move to shard replica sets rather than trees.
  private lazy val childrenWithPlaceholder = if (children.isEmpty) {
    Seq(LeafRoutingNode.NullNode.asInstanceOf[RoutingNode[T]])
  } else {
    children
  }

  protected[shards] final def collectedShards(readOnly: Boolean, rb: Behavior, wb: Behavior) = {
    childrenWithPlaceholder flatMap {
      // apply the strongest of the Leaf behavior and the parent behavior
      _.collectedShards(readOnly, rb, wb).map { leaf =>
        // take the strongest behavior in the path to this leaf
        leaf.copy(
          readBehavior = Behavior.Ordering.max(this.readBehavior, rb),
          writeBehavior = Behavior.Ordering.max(this.writeBehavior, wb)
        )
      }
    }
  }
}


// BlockedShard. Refuse and fail all traffic.

case class BlockedShard[T](shardInfo: ShardInfo, weight: Weight, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def readBehavior = Deny
  protected def writeBehavior = Deny
}


// BlackHoleShard. Silently refuse all traffic.

case class BlackHoleShard[T](shardInfo: ShardInfo, weight: Weight, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def readBehavior = Ignore
  protected def writeBehavior = Ignore
}


// WriteOnlyShard. Fail all read traffic.

case class WriteOnlyShard[T](shardInfo: ShardInfo, weight: Weight, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def readBehavior = Deny
  protected def writeBehavior = Allow
}


// ReadOnlyShard. Fail all write traffic.

case class ReadOnlyShard[T](shardInfo: ShardInfo, weight: Weight, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def readBehavior = Allow
  protected def writeBehavior = Deny
}


// SlaveShard. Silently refuse all write traffic.

case class SlaveShard[T](shardInfo: ShardInfo, weight: Weight, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T] {
  protected def readBehavior = Allow
  protected def writeBehavior = Ignore
}
