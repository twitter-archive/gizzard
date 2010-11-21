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
import net.lag.logging.Logger


class ReplicatingJsonCodec(relay: => JobRelay, unparsable: Array[Byte] => Unit)
extends JsonCodec[JsonJob](unparsable) {
  lazy val innerCodec = {
    val c = new JsonCodec[JsonJob](unparsable)
    c += ("ReplicatingJob".r -> new ReplicatingJobParser(c, relay))
    c += ("ReplicatedJob".r -> new ReplicatedJobParser(c))
    c
  }

  override def +=(item: (Regex, JsonJobParser[JsonJob])) = innerCodec += item
  override def +=(r: Regex, p: JsonJobParser[JsonJob])   = innerCodec += ((r, p))

  override def inflate(json: Map[String, Any]): JsonJob = {
    innerCodec.inflate(json) match {
      case j: ReplicatingJob[_] => j
      case j => if (j.shouldReplicate) {
        new ReplicatingJob(relay, List(j))
      } else {
        j
      }
    }
  }
}

class ReplicatedJob[J <: JsonJob](jobs: Iterable[J]) extends JsonNestedJob(jobs) {
  override val shouldReplicate = false
}

class ReplicatedJobParser[J <: JsonJob](codec: JsonCodec[J]) extends JsonJobParser[J] {
  type Tasks = Iterable[Map[String, Any]]

  override def apply(json: Map[String, Any]) = {
    val tasks = json("tasks").asInstanceOf[Tasks].map(codec.inflate)
    new ReplicatedJob(tasks).asInstanceOf[J]
  }
}

class ReplicatingJob[J <: JsonJob](
  relay: JobRelay,
  jobs: Iterable[J],
  clusters: Iterable[String],
  serialized: Iterable[String])
extends JsonNestedJob(jobs) {

  def this(relay: JobRelay, jobs: Iterable[J], clusters: Iterable[String]) =
    this(relay, jobs, clusters, jobs.map(_.toJson))

  def this(relay: JobRelay, jobs: Iterable[J]) = this(relay, jobs, relay.clusters)

  private val clustersQueue = new Queue[String]
  clustersQueue ++= clusters

  override def toMap: Map[String, Any] = {
    var attrs = super.toMap.toList
    if (!clustersQueue.isEmpty) attrs = "dest_clusters" -> clustersQueue.toList :: attrs
    if (!serialized.isEmpty)    attrs = "serialized" -> serialized :: attrs
    Map(attrs: _*)
  }

  // XXX: do this work in parallel in a future pool.
  override def apply() {
    var ex: Throwable = null

    try { replicateToClusters() } catch { case e: Throwable => ex = e }
    super.apply()

    if (ex ne null) throw ex
  }

  private def replicateToClusters() {
    while (!clustersQueue.isEmpty) {
      val c = clustersQueue.dequeue()
      try { relay(c)(List(serialized)) } catch {
        case e: Throwable => { clustersQueue += c; throw e }
      }
    }
  }
}

class ReplicatingJobParser[J <: JsonJob](
  codec: JsonCodec[J],
  relay: => JobRelay)
extends JsonJobParser[J] {
  type Tasks = Iterable[Map[String, Any]]

  override def apply(json: Map[String, Any]) = {
    val clusters   = json.get("dest_clusters").map(_.asInstanceOf[Iterable[String]]) getOrElse Nil
    val serialized = json.get("serialized").map(_.asInstanceOf[Iterable[String]]) getOrElse Nil
    val tasks      = json("tasks").asInstanceOf[Tasks].map(codec.inflate)

    new ReplicatingJob(relay, tasks, clusters, serialized).asInstanceOf[J]
  }
}

