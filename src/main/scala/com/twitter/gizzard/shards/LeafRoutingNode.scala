package com.twitter.gizzard.shards


trait ShardFactory[+T] {
  def instantiate(shardInfo: ShardInfo, weight: Int): T
  def instantiateReadOnly(shardInfo: ShardInfo, weight: Int): T
  def materialize(shardInfo: ShardInfo)
}

class LeafRoutingNodeFactory[T](shardFactory: ShardFactory[T]) extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    val factory = shardFactory
    new LeafRoutingNode(factory, shardInfo, weight)
  }

  override def materialize(shardInfo: ShardInfo) {
    shardFactory.materialize(shardInfo)
  }
}

object LeafRoutingNode {
  private class WrapperShardFactory[T](readOnlyShard: => T, readWriteShard: => T) extends ShardFactory[T] {
    def instantiate(shardInfo: ShardInfo, weight: Int)         = readWriteShard
    def instantiateReadOnly(shardInfo: ShardInfo, weight: Int) = readOnlyShard
    def materialize(shardInfo: ShardInfo) {}
  }

  // convenience constructors for manual tree creation.
  def apply[T](readOnlyShard: T, readWriteShard: T, info: ShardInfo, weight: Int): LeafRoutingNode[T] = {
    new LeafRoutingNode(new WrapperShardFactory(readOnlyShard, readWriteShard), info, weight)
  }

  def apply[T](shard: T): LeafRoutingNode[T] = {
    apply(shard, shard, new ShardInfo("", "", ""), 1)
  }

  // XXX: remove when we move to shard replica sets rather than trees.
  object NullNode extends LeafRoutingNode[Null](
    new LeafRoutingNode.WrapperShardFactory(null, null),
    new ShardInfo("NullShardPlaceholder", "null", "null"),
    1
  )
}

class LeafRoutingNode[T](private[shards] val factory: ShardFactory[T], val shardInfo: ShardInfo, val weight: Int) extends RoutingNode[T] {

  import RoutingNode._

  // convenience constructor for manual creation.
  def this(factory: ShardFactory[T], weight: Int) = this(factory, new ShardInfo("", "", ""), weight)

  val children = Nil

  // only one of these will usually be called.
  val readOnlyShard  = factory.instantiateReadOnly(shardInfo, weight)
  val readWriteShard = factory.instantiate(shardInfo, weight)

  override def shardInfos = Seq(shardInfo)

  protected[shards] def collectedShards(readOnly: Boolean) = {
    Seq(Leaf(shardInfo, Allow, Allow, if (readOnly) readOnlyShard else readWriteShard))
  }

  override def equals(other: Any) = other match {
    case n: LeafRoutingNode[_] => {
      (shardInfo == n.shardInfo) &&
      (weight    == n.weight)    &&
      (factory   == n.factory)
    }
    case _ => false
  }

  override def hashCode() = shardInfo.hashCode
}

