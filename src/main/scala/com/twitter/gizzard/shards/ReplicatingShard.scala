package com.twitter.gizzard.shards

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


class ReplicatingShardFactory[ConcreteShard <: Shard](
      readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard,
      future: Option[Future],
      timeout: Duration)
  extends shards.ShardFactory[ConcreteShard] {

  def instantiate(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[ConcreteShard]) =
    readWriteShardAdapter(new ReplicatingShard(
      shardInfo,
      weight,
      replicas,
      new LoadBalancer(replicas),
      future,
      timeout
    ))

  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReplicatingShard[ConcreteShard <: Shard](
      val shardInfo: ShardInfo,
      val weight: Int,
      val children: Seq[ConcreteShard],
      val loadBalancer: (() => Seq[ConcreteShard]),
      val future: Option[Future],
      val futureTimeout: Duration)
  extends ReadWriteShard[ConcreteShard] {

  def readOperation[A](method: (ConcreteShard => A)) = failover(method(_), loadBalancer())
  def writeOperation[A](method: (ConcreteShard => A)) = fanoutWrite(method, children)
  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
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

  protected def fanoutWriteFuture[A](method: (ConcreteShard => A), replicas: Seq[ConcreteShard], future: Future): A = {
    val exceptions = new mutable.ArrayBuffer[Throwable]()
    val results = new mutable.ArrayBuffer[A]()

    replicas.map { replica => (replica.shardInfo, future(method(replica))) }.map { case (shardInfo, future) =>
      try {
        results += future.get(futureTimeout.inMillis, TimeUnit.MILLISECONDS)
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

  protected def fanoutWriteSerial[A](method: (ConcreteShard => A), replicas: Seq[ConcreteShard]): A = {
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

  protected def fanoutWrite[A](method: (ConcreteShard => A), replicas: Seq[ConcreteShard]): A = {
    future match {
      case None => fanoutWriteSerial(method, replicas)
      case Some(f) => fanoutWriteFuture(method, replicas, f)
    }
  }

  protected def failover[A](f: ConcreteShard => A, replicas: Seq[ConcreteShard]): A = {
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

  protected def rebuildableFailover[A](f: ConcreteShard => Option[A], rebuild: (ConcreteShard, ConcreteShard) => Unit,
                                       replicas: Seq[ConcreteShard], toRebuild: List[ConcreteShard],
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
