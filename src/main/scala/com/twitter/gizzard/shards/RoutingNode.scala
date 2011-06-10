package com.twitter.gizzard.shards

import java.lang.reflect.UndeclaredThrowableException
import java.util.concurrent.{ExecutionException, TimeoutException}
import com.twitter.util.{Try, Return, Throw}
import com.twitter.logging.Logger


abstract class RoutingNodeFactory[T] {
  def instantiate(shardInfo: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]): RoutingNode[T]
  def materialize(shardInfo: ShardInfo) {}
}

// Turn case class or other generic constructors into node factories.
class ConstructorRoutingNodeFactory[T](constructor: (ShardInfo, Int, Seq[RoutingNode[T]]) => RoutingNode[T])
extends RoutingNodeFactory[T] {
  def instantiate(info: ShardInfo, weight: Int, children: Seq[RoutingNode[T]]) = constructor(info, weight, children)
}

protected[shards] object RoutingNode {
  // XXX: use real behavior once ShardStatus lands
  sealed trait Behavior
  case object Allow extends Behavior
  case object Deny extends Behavior
  case object Ignore extends Behavior
  case class Leaf[T](info: ShardInfo, readBehavior: Behavior, writeBehavior: Behavior, shard: T)
}

abstract class RoutingNode[T] {

  import RoutingNode._

  def shardType = shardInfo.className // XXX: replace with some other thing
  def shardInfo: ShardInfo
  def weight: Int
  def children: Seq[RoutingNode[T]]
  protected[shards] def collectedShards(readOnly: Boolean): Seq[Leaf[T]]

  protected val log = Logger.get

  def shardInfos: Seq[ShardInfo] = children flatMap { _.shardInfos }

  protected def nodeSetFromCollected(readOnly: Boolean) = {
    val m = collectedShards(readOnly) groupBy { l =>
      if (readOnly) l.readBehavior else l.writeBehavior
    }

    val active  = m.getOrElse(Allow, Nil) map { l => (l.info, l.shard) }
    val blocked = m.getOrElse(Deny, Nil) map { _.info }
    new NodeSet(shardInfo, active, blocked)
  }

  def read = nodeSetFromCollected(true)

  def write = nodeSetFromCollected(false)

  def skip(ss: ShardId*): RoutingNode[T] = if (ss.toSet.contains(shardInfo.id)) {
    BlackHoleShard(shardInfo, weight, Seq(this))
  } else {
    this
  }

  @deprecated("use read.all instead")
  def readAllOperation[A](f: T => A): Seq[Either[Throwable,A]] = read.all(f) map { f => Try(f()) } map {
    case Return(r) => Right(r)
    case Throw(e)  => Left(e)
  }

  @deprecated("use read.any instead")
  def readOperation[A](f: T => A) = read.tryAny { shard =>
    Try(f(shard)) onFailure { e => logException(e, shard) }
  }

  @deprecated("use write.all instead")
  def writeOperation[A](f: T => A) = {
    var rv: Option[A] = None
    write foreach { s => rv = Some(f(s)) }
    rv.getOrElse(throw new ShardBlackHoleException(shardInfo.id))
  }

  @deprecated("reimplement using read.iterator instead")
  def rebuildableReadOperation[A](f: T => Option[A])(rebuild: (T, T) => Unit): Option[A] = {
    val iter = read.iterator

    var everSuccessful     = false
    var toRebuild: List[T] = Nil

    while (iter.hasNext) {
      val shard = iter.next

      try {
        val result = f(shard)
        everSuccessful = true

        if (result.isEmpty) {
          toRebuild = shard :: toRebuild
        } else {
          toRebuild.foreach(rebuild(shard, _))
          return result
        }
      } catch {
        case e => logException(e, shard)
      }
    }

    if (everSuccessful) {
      None
    } else {
      throw new ShardOfflineException(shardInfo.id)
    }
  }

  protected def logException(e: Throwable, shard: T) {
    val shardId    = (collectedShards(false) find { l => l.shard == shard }).get.info.id
    val normalized = normalizeException(e, shardId)

    log.warning(e, "Error on %s: %s", shardId, e)
  }

  protected def normalizeException(ex: Throwable, shardId: ShardId): Throwable = ex match {
    case e: ExecutionException => normalizeException(e.getCause, shardId)
    // fondly known as JavaOutrageException
    case e: UndeclaredThrowableException => normalizeException(e.getCause, shardId)
    case e: TimeoutException => new ReplicatingShardTimeoutException(shardId, e)
    case e => e
  }

  // equals overrides

  override def equals(other: Any) = other match {
    case n: RoutingNode[_] => {
      (shardInfo == n.shardInfo) &&
      (weight    == n.weight)    &&
      (children  == n.children)
    }
    case _ => false
  }

  override def hashCode() = children.hashCode
}
