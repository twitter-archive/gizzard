package com.twitter.gizzard
package shards

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import java.util.Random
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.util.Duration
import net.lag.logging.Logger


class ReplicatingShardFactory[T](future: Option[Future]) extends RoutingNodeFactory[T] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[RoutingNode[T]]) = {
    new ReplicatingShard(
      shardInfo,
      weight,
      replicas,
      new LoadBalancer(replicas),
      future
    )
  }
}

class ReplicatingShard[T](
  val shardInfo: ShardInfo,
  val weight: Int,
  val children: Seq[RoutingNode[T]],
  val loadBalancer: (() => Seq[RoutingNode[T]]),
  val future: Option[Future])
extends RoutingNode[T] {

  lazy val log = Logger.get

  override def skipShard(ss: ShardId*) = {
    val filtered = children.filterNot(ss.toSet.contains)

    if (filtered.isEmpty) {
      BlackHoleShard(shardInfo, weight, children)
    } else if (filtered.size == children.size) {
      this
    } else {
      new ReplicatingShard[T](shardInfo, weight, filtered, new LoadBalancer(filtered), future)
    }
  }

  def readAllOperation[A](f: T => A) = fanout(children)(_.readAllOperation(f))

  def readOperation[A](f: T => A) = failover(loadBalancer())(_.readOperation(f))

  def writeOperation[A](f: T => A) = {
    val allResults = fanout(children) { c =>
      try {
        Seq(Right(c.writeOperation(f)))
      } catch {
        case e => normalizeException(e, shardInfo.id).map(Left(_)).toSeq
      }
    } map {
      case Left(e) => throw e
      case Right(result) => result
    }

    allResults.headOption getOrElse {
      throw new ShardBlackHoleException(shardInfo.id)
    }
  }

  protected def fanout[A](replicas: Seq[RoutingNode[T]])(f: RoutingNode[T] => Seq[Either[Throwable,A]]) = {
    future match {
      case None => replicas flatMap f
      case Some(future) => {
        replicas.map { r => Pair(r, future(f(r))) } flatMap { case (replica, task) =>
          try {
            task.get(future.timeout.inMillis, TimeUnit.MILLISECONDS)
          } catch {
            case e => normalizeException(e, replica.shardInfo.id).map(Left(_)).toSeq
          }
        }
      }
    }
  }

  protected def failover[A](replicas: Seq[RoutingNode[T]])(f: RoutingNode[T] => A): A = {
    replicas foreach { replica =>
      try {
        return f(replica)
      } catch {
        case e: ShardRejectedOperationException => ()
        case e: ShardException => log.warning(e, "Error on %s: %s", replica.shardInfo.id, e)
      }
    }

    throw new ShardOfflineException(shardInfo.id)
  }

  protected[shards] def rebuildRead[A](toRebuild: List[T])(f: (T, Seq[T]) => Option[A]): Either[List[T],A] = {
    val start: Either[List[T],A] = Left(toRebuild)
    var everSuccessful           = false

    val rv = (children foldLeft start) { (result, replica) =>
      result match {
        case Right(rv)       => return Right(rv)
        case Left(toRebuild) => try {
          val next = replica.rebuildRead(toRebuild)(f)
          everSuccessful = true
          next
        } catch {
          case e: ShardRejectedOperationException => Left(toRebuild)
          case e: ShardException => {
            log.warning(e, "Error on %s: %s", replica.shardInfo.id, e)
            Left(toRebuild)
          }
        }
      }
    }

    if (!everSuccessful) {
      throw new ShardOfflineException(shardInfo.id)
    } else {
      rv
    }
  }
}
