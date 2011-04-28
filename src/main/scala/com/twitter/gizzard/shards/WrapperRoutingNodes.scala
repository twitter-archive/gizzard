package com.twitter.gizzard.shards


// Base class for all read/write flow wrapper shards

abstract class WrapperRoutingNode[T] extends RoutingNode[T] {

  val inner = children.head

  def readAllOperation[A](f: T => A) = inner.readAllOperation(f)
  def readOperation[A](f: T => A) = inner.readOperation(f)
  def writeOperation[A](f: T => A) = inner.writeOperation(f)

  protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    inner.rebuildRead(toRebuild)(f)
  }
}


// BlockedShard. Refuse and fail all traffic.

case class BlockedShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {

  protected def exception = new ShardRejectedOperationException("shard is offline", shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq[Either[Throwable,A]](Left(exception))
  override def readOperation[A](method: T => A)    = throw exception
  override def writeOperation[A](method: T => A)   = throw exception

  override protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    throw exception
  }
}



// BlackHoleShard. Silently refuse all traffic.

case class BlackHoleShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {

  protected def exception = throw new ShardBlackHoleException(shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq[Either[Throwable,A]]()
  override def readOperation[A](method: T => A)    = throw exception
  override def writeOperation[A](method: T => A)   = throw exception

  override protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    throw exception
  }
}



// WriteOnlyShard. Fail all read traffic.

case class WriteOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {

  private def exception = new ShardRejectedOperationException("shard is write-only", shardInfo.id)

  override def readAllOperation[A](method: T => A) = Seq(Left(exception))
  override def readOperation[A](method: T => A)    = throw exception
  override def rebuildableReadOperation[A](method: T => Option[A])(rebuild: (T, T) => Unit) = throw exception
}



// ReadOnlyShard. Fail all write traffic.

case class ReadOnlyShard[T](shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) extends WrapperRoutingNode[T] {

  override def writeOperation[A](method: T => A) = {
    throw new ShardRejectedOperationException("shard is read-only", shardInfo.id)
  }
}
