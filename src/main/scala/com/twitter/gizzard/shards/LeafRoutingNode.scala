package com.twitter.gizzard.shards


class LeafRoutingNode[T](shard: T, val shardInfo: ShardInfo, val weight: Int) extends RoutingNode[T] {

  import RoutingNode._

  val children = Nil

  // convenience constructor for manual creation.
  def this(shard: T, weight: Int) = this(shard, new ShardInfo("", "", ""), weight)

  protected[shards] def collectedShards = Seq(Leaf(shardInfo, Allow, Allow, shard))

  protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    f(shard, toRebuild) match {
      case Some(rv) => Right(rv)
      case None     => Left(shard :: toRebuild)
    }
  }
}

class LeafRoutingNodeFactory[T](shardFactory: ShardFactory[T]) extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    val shard = shardFactory.instantiate(shardInfo, weight)
    new LeafRoutingNode(shard, shardInfo, weight)
  }

  override def materialize(shardInfo: ShardInfo) {
    shardFactory.materialize(shardInfo)
  }
}
