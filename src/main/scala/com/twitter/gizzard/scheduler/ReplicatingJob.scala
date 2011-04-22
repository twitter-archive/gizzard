package com.twitter.gizzard
package scheduler

import java.util.{LinkedList => JLinkedList}
import java.nio.ByteBuffer
import scala.collection.mutable.Queue
import scala.util.matching.Regex
import com.twitter.ostrich.stats.{JsonStats, StatsProvider}
import com.twitter.logging.Logger
import com.twitter.util.Duration

import thrift.conversions.Sequences._
import nameserver.JobRelay


class ReplicatingJsonCodec(relay: => JobRelay, unparsable: Array[Byte] => Unit)
extends JsonCodec(unparsable) {
  processors.clear()

  lazy val innerCodec = {
    val c = new JsonCodec(unparsable)
    c += ("ReplicatingJob".r -> new ReplicatingJobParser(c, relay))
    c += ("ReplicatedJob".r -> new ReplicatedJobParser(c))
    c
  }

  override def +=(item: (Regex, JsonJobParser)) = innerCodec += item
  override def +=(r: Regex, p: JsonJobParser)   = innerCodec += ((r, p))

  override def inflate(json: Map[String, Any]): JsonJob = {
    innerCodec.inflate(json) match {
      case j: ReplicatingJob => j
      case j => if (j.shouldReplicate) {
        new ReplicatingJob(relay, List(j))
      } else {
        j
      }
    }
  }
}

class LoggingJsonCodec(codec: JsonCodec, conf: config.StatsCollection) extends JsonCodec(codec.unparsableJobHandler) {
/*  private val proxyFactory = {
    val sampledQueryCollection = new JsonStats(Logger.get(conf.sampledQueryLoggerName))
    val slowQueryCollection = new JsonStats(Logger.get(conf.slowQueryLoggerName))


    conf("jobs", 
    new JobLoggingProxy[JsonJob](slowQueryCollection, conf.slowQueryThreshold, sampledQueryCollection, conf.sampledQueryRate)
  } */

  override def +=(item: (Regex, JsonJobParser)) = codec += item
  override def +=(r: Regex, p: JsonJobParser)   = codec += ((r, p))
  override def inflate(json: Map[String, Any]): JsonJob = conf("jobs", codec.inflate(json))
}

class ReplicatedJob(jobs: Iterable[JsonJob]) extends JsonNestedJob(jobs) {
  override val shouldReplicate = false
}

class ReplicatedJobParser(codec: JsonCodec) extends JsonJobParser {
  type Tasks = Iterable[Map[String, Any]]

  override def apply(json: Map[String, Any]) = {
    val tasks = json("tasks").asInstanceOf[Tasks].map(codec.inflate)
    new ReplicatedJob(tasks)
  }
}

class ReplicatingJob(
  relay: JobRelay,
  jobs: Iterable[JsonJob],
  clusters: Iterable[String],
  serialized: Iterable[Array[Byte]])
extends JsonNestedJob(jobs) {

  def this(relay: JobRelay, jobs: Iterable[JsonJob], clusters: Iterable[String]) =
    this(relay, jobs, clusters, jobs.map(_.toJsonBytes))

  def this(relay: JobRelay, jobs: Iterable[JsonJob]) = this(relay, jobs, relay.clusters)

  private val clustersQueue = new Queue[String]
  clustersQueue ++= clusters

  override def toMap: Map[String, Any] = {
    var attrs = super.toMap

    if (!clustersQueue.isEmpty) attrs += ("dest_clusters" -> clustersQueue.toList)
    if (!serialized.isEmpty)    attrs += ("serialized" -> serialized.map(new String(_, "UTF-8")))

    attrs.toMap
  }

  // XXX: do this work in parallel in a future pool.
  override def apply() {
    var ex: Throwable = null

    try { replicateToClusters() } catch { case e: Throwable => ex = e }
    super.apply()

    if (ex ne null) throw ex
  }

  private def replicateToClusters() {
    val badClusters = new Queue[String]
    var ex: Throwable = null

    while (!clustersQueue.isEmpty && !serialized.isEmpty) {
      val c = clustersQueue.dequeue()
      try { relay(c)(serialized) } catch {
        case e: Throwable => { badClusters += c; ex = e }
      }
    }

    clustersQueue ++= badClusters

    if (ex ne null) throw ex
  }
}

class ReplicatingJobParser(
  codec: JsonCodec,
  relay: => JobRelay)
extends JsonJobParser {
  type Tasks = Iterable[Map[String, Any]]

  override def apply(json: Map[String, Any]): JsonJob = {
    val clusters   = json.get("dest_clusters").map(_.asInstanceOf[Iterable[String]]) getOrElse Nil
    val serialized = json.get("serialized").map(_.asInstanceOf[Iterable[String]].map(_.getBytes("UTF-8"))) getOrElse Nil
    val tasks      = json("tasks").asInstanceOf[Tasks].map(codec.inflate)

    new ReplicatingJob(relay, tasks, clusters, serialized)
  }
}

