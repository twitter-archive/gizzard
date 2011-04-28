package com.twitter.gizzard.shards


class LeafRoutingNode[T](shard: T, val shardInfo: ShardInfo, val weight: Int) extends RoutingNode[T] {

  val children = Nil

  // convenience constructor for manual creation.
  def this(shard: T, weight: Int) = this(shard, new ShardInfo("", "", ""), weight)

  def readAllOperation[A](f: T => A) = Seq(try { Right(f(shard)) } catch { case e => Left(e) })
  def readOperation[A](f: T => A) = f(shard)
  def writeOperation[A](f: T => A) = f(shard)

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
