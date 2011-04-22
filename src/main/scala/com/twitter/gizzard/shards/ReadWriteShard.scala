package com.twitter.gizzard.shards

import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent.{ExecutionException, TimeoutException}


abstract class RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]): RoutingNode[T]
  def materialize(shardInfo: ShardInfo) {}
}

abstract class RoutingNode[T] {
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[RoutingNode[T]]

  def readAllOperation[A](f: T => A): Seq[Either[Throwable,A]]
  def readOperation[A](f: T => A): A
  def writeOperation[A](f: T => A): A

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


class PassThroughNode[T](val shardInfo: ShardInfo, val weight: Int, val children: Seq[RoutingNode[T]])
extends RoutingNode[T] {

  val inner = children.head

  def readAllOperation[A](f: T => A) = inner.readAllOperation(f)
  def readOperation[A](f: T => A) = inner.readOperation(f)
  def writeOperation[A](f: T => A) = inner.writeOperation(f)

  protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]) = {
    inner.rebuildRead(toRebuild)(f)
  }
}


class LeafRoutingNode[T](val shardInfo: ShardInfo, val weight: Int, shard: T) extends RoutingNode[T] {
  val children: Seq[RoutingNode[T]] = Seq()

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
    new LeafRoutingNode(shardInfo, weight, shardFactory.instantiate(shardInfo))
  }

  override def materialize(shardInfo: ShardInfo) {
    shardFactory.materialize(shardInfo)
  }
}
