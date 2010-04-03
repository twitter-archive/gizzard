package com.twitter.gizzard.shards

import java.sql.SQLException
import java.util.Random
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.Sorting
import net.lag.logging.{Logger, ThrottledLogger}
import com.twitter.gizzard.thrift.conversions.Sequences._


class ReplicatingShard[ConcreteShard <: Shard](val shardInfo: ShardInfo, val weight: Int,
  val children: Seq[ConcreteShard], loadBalancer: (() => Seq[ConcreteShard]),
  log: ThrottledLogger[String], future: Future)
  extends ReadWriteShard[ConcreteShard] {

  def readOperation[A](method: (ConcreteShard => A)) = failover(method(_), loadBalancer())
  def writeOperation[A](method: (ConcreteShard => A)) = fanoutWrite(method, children)

  private def fanoutWrite[A](method: (ConcreteShard => A), replicas: Seq[ConcreteShard]): A = {
    val exceptions = new mutable.ArrayBuffer[Exception]()
    val results = new mutable.ArrayBuffer[A]()

    children.map { replica => future(method(replica.asInstanceOf[ConcreteShard])) }.map { future =>
      try {
        results += future.get(6000, TimeUnit.MILLISECONDS)
      } catch {
        case e: Exception =>
          exceptions += e
      }
    }
    exceptions.map { throw _ }
    results.first
  }

  private def failover[A](f: ConcreteShard => A, replicas: Seq[ConcreteShard]): A = {
    replicas match {
      case Seq() => throw new ShardOfflineException
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
                log.error(shardId, e, "Error on %s: %s", shardId, e)
            }
            failover(f, remainder)
        }
      }
  }
}
