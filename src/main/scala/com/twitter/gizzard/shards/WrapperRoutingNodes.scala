package com.twitter.gizzard.shards


// Base class for all read/write flow wrapper shards

class WrapperRoutingNode[T](val shardInfo: ShardInfo, val weight: Int, val children: Seq[RoutingNode[T]])
extends RoutingNode[T] {

  val inner = children.head

  def readAllOperation[A](f: T => A) = inner.readAllOperation(f)
  def readOperation[A](f: T => A) = inner.readOperation(f)
  def writeOperation[A](f: T => A) = inner.writeOperation(f)

  protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    inner.rebuildRead(toRebuild)(f)
  }
}


// BlockedShard. Refuse and fail all traffic.

class BlockedShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new BlockedShard(shardInfo, weight, children)
  }
}

class BlockedShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T](shardInfo, weight, children) {

  protected def exception = new ShardRejectedOperationException("shard is offline", shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq[Either[Throwable,A]](Left(exception))
  override def readOperation[A](method: T => A)    = throw exception
  override def writeOperation[A](method: T => A)   = throw exception

  override protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    throw exception
  }
}



// BlackHoleShard. Silently refuse all traffic.

class BlackHoleShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new BlackHoleShard(shardInfo, weight, children)
  }
}

class BlackHoleShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends BlockedShard[T](shardInfo, weight, children) {

  override protected def exception = throw new ShardBlackHoleException(shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq[Either[Throwable,A]]()
}



// WriteOnlyShard. Fail all read traffic.

class WriteOnlyShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new WriteOnlyShard(shardInfo, weight, children)
  }
}

class WriteOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T](shardInfo, weight, children) {

  private def exception = new ShardRejectedOperationException("shard is write-only", shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq(Left(exception))
  override def readOperation[A](method: T => A)    = throw exception
  override def rebuildableReadOperation[A](method: T => Option[A])(rebuild: (T, T) => Unit) = throw exception
}



// ReadOnlyShard. Fail all write traffic.

class ReadOnlyShardFactory[T] extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = {
    new ReadOnlyShard(shardInfo, weight, children)
  }
}

class ReadOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]])
extends WrapperRoutingNode[T](shardInfo, weight, children) {

  override def writeOperation[A](method: T => A) = {
    throw new ShardRejectedOperationException("shard is read-only", shardInfo.id)
  }
}
