package com.twitter.gizzard.shards

import java.lang.reflect.UndeclaredThrowableException
import java.sql.SQLException
import java.util.Random
import java.util.concurrent.{ExecutionException, TimeUnit}
import scala.collection.mutable
import scala.util.Sorting
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.gizzard.thrift.conversions.Sequences._
import net.lag.logging.Logger


class ReplicatingShardFactory[ConcreteShard <: Shard](
      readWriteShardAdapter: ReadWriteShard[ConcreteShard] => ConcreteShard,
      future: Future) extends shards.ShardFactory[ConcreteShard] {
  def instantiate(shardInfo: shards.ShardInfo, weight: Int, replicas: Seq[ConcreteShard]) =
    readWriteShardAdapter(new ReplicatingShard(shardInfo, weight, replicas, new LoadBalancer(replicas), future))
  def materialize(shardInfo: shards.ShardInfo) = ()
}

class ReplicatingShard[ConcreteShard <: Shard](val shardInfo: ShardInfo, val weight: Int,
  val children: Seq[ConcreteShard], val loadBalancer: (() => Seq[ConcreteShard]),
  val future: Future)
  extends ReadWriteShard[ConcreteShard] {

  def readOperation[A](method: (ConcreteShard => A)) = failover(method(_), loadBalancer())
  def writeOperation[A](method: (ConcreteShard => A)) = fanoutWrite(method, children)
  def rebuildableReadOperation[A](method: (ConcreteShard => Option[A]))(rebuild: (ConcreteShard, ConcreteShard) => Unit) =
    rebuildableFailover(method, rebuild, loadBalancer(), Nil, false)

  lazy val log = Logger.get

  protected def fanoutWrite[A](method: (ConcreteShard => A), replicas: Seq[ConcreteShard]): A = {
    val exceptions = new mutable.ArrayBuffer[Throwable]()
    val results = new mutable.ArrayBuffer[A]()

    children.map { replica => future(method(replica.asInstanceOf[ConcreteShard])) }.map { future =>
      try {
        results += future.get(6000, TimeUnit.MILLISECONDS)
      } catch {
        case e: ExecutionException => // this should be unwrapped
          e.getCause() match {
            case ute: UndeclaredThrowableException => // this java OutrageException should be unwrapped as well.
              exceptions += ute.getCause()
            case ex: Exception =>
              exceptions += ex
          }
        case e: Exception =>
          exceptions += e
      }
    }
    exceptions.map { throw _ }
    results.first
  }

  protected def failover[A](f: ConcreteShard => A, replicas: Seq[ConcreteShard]): A = {
    replicas match {
      case Seq() =>
        throw new ShardOfflineException
      case Seq(shard, remainder @ _*) =>
        try {
          f(shard)
        } catch {
          case e: ShardException =>
            val shardInfo = shard.shardInfo
            val shardId = shardInfo.hostname + "/" + shardInfo.tablePrefix
            e match {
              case _: ShardRejectedOperationException =>
              case _ =>
                log.error(e, "Error on %s: %s", shardId, e)
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
          throw new ShardOfflineException
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
            val shardInfo = shard.shardInfo
            val shardId = shardInfo.hostname + "/" + shardInfo.tablePrefix
            e match {
              case _: ShardRejectedOperationException =>
              case _ =>
                log.error(e, "Error on %s: %s", shardId, e)
            }
            rebuildableFailover(f, rebuild, remainder, toRebuild, everSuccessful)
        }
    }
  }
}
