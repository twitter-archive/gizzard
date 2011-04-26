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
import com.twitter.util.{Time, Duration}
import com.twitter.logging.Logger


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

  def readAllOperation[A](method: (S => A)) = fanout(method(_), children)
  def readOperation[A](method: (S => A)) = failover(method(_), loadBalancer())

  def writeOperation[A](method: (S => A)) = {
    fanout(method, children).map {
      case Left(ex)      => throw ex
      case Right(result) => result
    }.headOption.getOrElse(throw new ShardBlackHoleException(shardInfo.id))
  }

  def rebuildableReadOperation[A](method: (S => Option[A]))(rebuild: (S, S) => Unit) =
    rebuildableFailover(method, rebuild, loadBalancer(), Nil, false)

  lazy val log = Logger.get(getClass.getName)
  lazy val exceptionLog = Logger.get("exception")

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

  protected def fanoutFuture[A](method: (S => A), replicas: Seq[S], future: Future) = {
    val results = new mutable.ArrayBuffer[Either[Throwable,A]]()

    replicas.map { replica => (replica, future(method(replica))) }.foreach { case (replica, futureTask) =>
      try {
        results += Right(futureTask.get(future.timeout.inMillis, TimeUnit.MILLISECONDS))
      } catch {
        case e: Exception =>
          unwrapException(e) match {
            case e: ShardBlackHoleException =>
              // nothing.
            case e: TimeoutException =>
              results += Left(new ReplicatingShardTimeoutException(replica.shardInfo.id, e))
            case e =>
              results += Left(e)
          }
      }
    }

    results
  }

  protected def fanoutSerial[A](method: (S => A), replicas: Seq[S]) = {
    val results = new mutable.ArrayBuffer[Either[Throwable,A]]()

    replicas.foreach { replica =>
      try {
        results += Right(method(replica))
      } catch {
        case e: ShardBlackHoleException =>
          // nothing.
        case e: TimeoutException =>
          results += Left(new ReplicatingShardTimeoutException(replica.shardInfo.id, e))
        case e => results += Left(e)
      }
    }

    results
  }

  protected def fanout[A](method: (S => A), replicas: Seq[S]) = {
    future match {
      case None => fanoutSerial(method, replicas)
      case Some(f) => fanoutFuture(method, replicas, f)
    }
  }

  protected def failover[A](f: S => A, replicas: Seq[S]): A = {
    replicas match {
      case Seq() =>
        throw new ShardOfflineException(shardInfo.id)
      case Seq(shard, remainder @ _*) =>
        val start = Time.now
        try {
          Stats.transaction.record("Reading from: " + shard.toString)
          f(shard)
        } catch {
          case e: ShardRejectedOperationException =>
            Stats.transaction.record("Rejected operation: "+e)
            failover(f, remainder)
          case e: ShardException =>
            Stats.transaction.record("Failed read: "+e)
            exceptionLog.warning(e, "Error on %s", shard.shardInfo.id)
//            log.warning(e, "Error on %s: %s", shard.shardInfo.id, e)
            failover(f, remainder)
        } finally {
          val duration = Time.now-start
          Stats.transaction.record("Total time on "+shard+": "+duration.inMillis)
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
          case e: ShardRejectedOperationException =>
            rebuildableFailover(f, rebuild, remainder, toRebuild, everSuccessful)
          case e: ShardException =>
            exceptionLog.warning(e, "Error on %s", shard.shardInfo.id)
//            log.warning(e, "Error on %s: %s", shard.shardInfo.id, e)
            rebuildableFailover(f, rebuild, remainder, toRebuild, everSuccessful)
        }
    }
  }
}
