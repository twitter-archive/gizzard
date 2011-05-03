package com.twitter.gizzard.shards

import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent.{ExecutionException, TimeoutException}


abstract class RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]): RoutingNode[T]
  def materialize(shardInfo: ShardInfo) {}
}

// Turn case class or other generic constructors into node factories.
class ConstructorRoutingNodeFactory[T](constructor: (ShardInfo, Int, Seq[RoutingNode[T]]) => RoutingNode[T])
extends RoutingNodeFactory[T] {
  def instantiate(info: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = constructor(info, weight, children)
}

abstract class RoutingNode[T] {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[RoutingNode[T]]

  def readAllOperation[A](f: T => A): Seq[Either[Throwable,A]]
  def readOperation[A](f: T => A): A
  def writeOperation[A](f: T => A): A


  def skipShard(ss: ShardId*): RoutingNode[T] = if (ss.toSet.contains(shardInfo.id)) {
    BlackHoleShard(shardInfo, weight, children)
  } else {
    this
  }


  protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]): Either[List[T],A]

  def rebuildableReadOperation[A](f: T => Option[A])(rebuild: (T, T) => Unit): Option[A] = {
    rebuildRead(Nil) { (shard, toRebuild) =>
      val result = f(shard)
      if (!result.isEmpty) toRebuild.foreach(rebuild(shard, _))
      result
    } match {
      case Left(s)   => None
      case Right(rv) => Some(rv)
    }
  }

  protected def normalizeException(ex: Throwable, shardId: ShardId): Option[Throwable] = ex match {
    case e: ExecutionException => normalizeException(e.getCause, shardId)
    // fondly known as JavaOutrageException
    case e: UndeclaredThrowableException => normalizeException(e.getCause, shardId)
    case e: ShardBlackHoleException => None
    case e: TimeoutException => Some(new ReplicatingShardTimeoutException(shardId, e))
    case e => Some(e)
  }
}
