package com.twitter.gizzard.scheduler

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer
import scala.collection.mutable.Queue
import scala.util.matching.Regex
import com.twitter.rpcclient.LoadBalancingChannel
import com.twitter.util.Duration
import thrift.{JobInjector, JobInjectorClient}
import thrift.conversions.Sequences._
import nameserver.JobRelay


class ReplicatingJsonCodec(relay: => JobRelay, unparsable: Array[Byte] => Unit)
extends JsonCodec[JsonJob](unparsable) {
  lazy val innerCodec = {
    val c = new JsonCodec[JsonJob](unparsable)
    c += ("RemoteReplicatingJob".r -> new RemoteReplicatingJobParser(c, relay))
    c
  }

  override def +=(item: (Regex, JsonJobParser[JsonJob])) = innerCodec += item
  override def +=(r: Regex, p: JsonJobParser[JsonJob])   = innerCodec += ((r, p))

  override def inflate(json: Map[String, Any]): JsonJob = {
    innerCodec.inflate(json) match {
      case j: RemoteReplicatingJob[_] => j
      case j => if (j.shouldReplicate) {
        new RemoteReplicatingJob(relay, List(j))
      } else {
        j
      }
    }
  }
}

class RemoteReplicatingJob[J <: JsonJob](
  relay: JobRelay,
  jobs: Iterable[J],
  destClusters: Iterable[String])
extends JsonNestedJob(jobs) {
  def this(relay: JobRelay, jobs: Iterable[J]) = this(relay, jobs, relay.clusters)

  val clustersQueue = {
    val q = new Queue[String]
    q ++= destClusters
    q
  }

  override def toMap: Map[String, Any] =
    Map("dest_clusters" -> clustersQueue.toList :: super.toMap.toList: _*)

  // XXX: do all this work in parallel in a future pool.
  override def apply() {
    while (!clustersQueue.isEmpty) {
      val c = clustersQueue.dequeue()
      try { relay(c)(List(this)) } catch {
        case e: Throwable => clustersQueue += c; throw e
      }
    }

    super.apply()
  }
}

class RemoteReplicatingJobParser[J <: JsonJob](
  codec: JsonCodec[J],
  relay: => JobRelay)
extends JsonJobParser[J] {
  type TaskJsons = Iterable[Map[String, Any]]

  override def apply(json: Map[String, Any]) = {
    val destClusters = json("dest_clusters").asInstanceOf[List[String]]
    val tasks = json("tasks").asInstanceOf[TaskJsons].map(codec.inflate)

    new RemoteReplicatingJob(relay, tasks, destClusters).asInstanceOf[J]
  }
}
