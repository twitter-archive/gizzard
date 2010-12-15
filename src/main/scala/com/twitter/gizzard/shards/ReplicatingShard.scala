package com.twitter.gizzard.shards

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import java.util.Random
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.logging.Logger
import com.twitter.xrayspecs.Duration
import com.twitter.xrayspecs.TimeConversions._


class ReplicatingShardFactory[S <: Shard](
      readWriteShardAdapter: ReadWriteShard[S] => S,
      future: Option[Future])
  extends shards.ShardFactory[S] {

  def instantiate(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[S]) =
    readWriteShardAdapter(new ReplicatingShard(
      shardInfo,
      weight,
      replicas,
      new LoadBalancer(replicas),
      future
    ))

  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReplicatingShard[S <: Shard](
      val shardInfo: ShardInfo,
      val weight: Int,
      val children: Seq[S],
      val loadBalancer: (() => Seq[S]),
      val future: Option[Future])
  extends ReadWriteShard[S] {

  def readOperation[A](method: (S => A)) = failover(method(_), loadBalancer())
  def writeOperation[A](method: (S => A)) = fanoutWrite(method, children)
  def rebuildableReadOperation[A](method: (S => Option[A]))(rebuild: (S, S) => Unit) =
    rebuildableFailover(method, rebuild, loadBalancer(), Nil, false)

  lazy val log = Logger.get

  protected def unwrapException(exception: Throwable): Throwable = {
    exception match {
      case e: ExecutionException =>
        unwrapException(e.getCause)
      case e: UndeclaredThrowableException =>
        // fondly known as JavaOutrageException
        unwrapException(e.getCause)
      case e =>
        e
    }
  }

  protected def fanoutWriteFuture[A](method: (S => A), replicas: Seq[S], future: Future): A = {
    val exceptions = new mutable.ArrayBuffer[Throwable]()
    val results = new mutable.ArrayBuffer[A]()

    replicas.map { replica => (replica.shardInfo, future(method(replica))) }.map { case (shardInfo, futureTask) =>
      try {
        results += futureTask.get(future.timeout.inMillis, TimeUnit.MILLISECONDS)
      } catch {
        case e: Exception =>
          unwrapException(e) match {
            case e: ShardBlackHoleException =>
              // nothing.
            case e: TimeoutException =>
              exceptions += new ReplicatingShardTimeoutException(shardInfo.id, e)
            case e =>
              exceptions += e
          }
      }
    }
    exceptions.map { throw _ }
    if (results.size == 0) {
      throw new ShardBlackHoleException(shardInfo.id)
    }
    results.first
  }

  protected def fanoutWriteSerial[A](method: (S => A), replicas: Seq[S]): A = {
    val results = replicas.flatMap { shard =>
      try {
        Some(method(shard))
      } catch {
        case e: ShardBlackHoleException =>
          None
      }
    }
    if (results.size == 0) {
      throw new ShardBlackHoleException(shardInfo.id)
    }
    results.first
  }

  protected def fanoutWrite[A](method: (S => A), replicas: Seq[S]): A = {
    future match {
      case None => fanoutWriteSerial(method, replicas)
      case Some(f) => fanoutWriteFuture(method, replicas, f)
    }
  }

  protected def failover[A](f: S => A, replicas: Seq[S]): A = {
    replicas match {
      case Seq() =>
        throw new ShardOfflineException(shardInfo.id)
      case Seq(shard, remainder @ _*) =>
        try {
          f(shard)
        } catch {
          case e: ShardException =>
            if (!e.isInstanceOf[ShardRejectedOperationException]) {
              log.warning(e, "Error on %s: %s", shard.shardInfo.id, e)
            }
            failover(f, remainder)
        }
      }
  }

  protected def rebuildableFailover[A](f: S => Option[A], rebuild: (S, S) => Unit,
                                       replicas: Seq[S], toRebuild: List[S],
                                       everSuccessful: Boolean): Option[A] = {
    replicas match {
      case Seq() =>
        if (everSuccessful) {
          None
        } else {
          throw new ShardOfflineException(shardInfo.id)
        }
      case Seq(shard, remainder @ _*) =>
        try {
          f(shard) match {
            case None =>
              rebuildableFailover(f, rebuild, remainder, shard :: toRebuild, true)
            case Some(answer) =>
              toRebuild.foreach { destShard => rebuild(shard, destShard) }
              Some(answer)
          }
        } catch {
          case e: ShardException =>
            if (!e.isInstanceOf[ShardRejectedOperationException]) {
              log.warning(e, "Error on %s: %s", shard.shardInfo.id, e)
            }
            rebuildableFailover(f, rebuild, remainder, toRebuild, everSuccessful)
        }
    }
  }
}
