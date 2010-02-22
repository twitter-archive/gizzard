package com.twitter.gizzard.shards

import java.sql.SQLException
import java.util.Random
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.util.Sorting
import net.lag.logging.{Logger, ThrottledLogger}
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.ostrich.W3CReporter


abstract class ReplicatingShard[ConcreteShard <: Shard]
  (val shardInfo: ShardInfo, val weight: Int, val children: Seq[Shard],
   log: ThrottledLogger[String], future: Future, eventLogger: Option[W3CReporter])
  extends ReadWriteShard[ConcreteShard] {

  private val random = new Random

  def readOperation[A](method: (ConcreteShard => A)) = failover(method(_))
  def writeOperation[A](method: (ConcreteShard => A)) = fanoutWrite(method)

  private def fanoutWrite[A](method: (ConcreteShard => A)): A = {
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

  case class ShardWithWeight(shard: Shard, weight: Float)

  private def computeWeights(replicas: Seq[Shard]) = {
    val totalWeight = replicas.foldLeft(0) { _ + _.weight }
    replicas.map { shard => ShardWithWeight(shard, shard.weight.toFloat / totalWeight.toFloat) }
  }

  private def getNext(randomNumber: Float, skipped: List[Shard],
                      replicas: Seq[ShardWithWeight]): (Shard, List[Shard]) = {
    val candidate = replicas.first
    if (replicas.size == 1 || randomNumber < candidate.weight) {
      (candidate.shard, skipped ++ replicas.drop(1).map { _.shard }.toList)
    } else {
      getNext(randomNumber - candidate.weight, skipped ++ List(candidate.shard), replicas.drop(1))
    }
  }

  def getNext(randomNumber: Float, children: Seq[Shard]): (Shard, List[Shard]) =
    getNext(randomNumber, Nil, computeWeights(children))

  private def failover[A](f: ConcreteShard => A): A = failover(f, children)

  private def failover[A](f: ConcreteShard => A, replicas: Seq[Shard]): A = {
    val (shard, remainder) = try {
      getNext(random.nextFloat, replicas)
    } catch {
      case e: NoSuchElementException =>
        throw new ShardException("All shard replicas are down")
    }

    try {
      f(shard.asInstanceOf[ConcreteShard])
    } catch {
      case e: ShardException =>
        val shardInfo = shard.shardInfo
        val shardId = shardInfo.hostname + "/" + shardInfo.tablePrefix
        e match {
          case _: ShardRejectedOperationException =>
          case _ =>
            log.error(shardId, e, "Error on %s: %s", shardId, e)
            // FIXME kind of an abuse of w3c. make a real event log.
            eventLogger.map { _.report(Map("db_error-" + shardId -> e.getClass.getName)) }
        }
        failover(f, remainder)
      case e: NoSuchElementException =>
        throw new ShardException("All shard replicas are down")
    }
  }
}
