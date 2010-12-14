package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import shards.{Shard, CopyableShard, CopyPage, ShardId, ShardDatabaseTimeoutException, ShardTimeoutException}


case class RemoteShardId(id: ShardId, cluster: String)

class CrossClusterCopyJobFactory[S <: CopyableShard[_]](
  ns: NameServer[S],
  s: JobScheduler[JsonJob],
  count: Int)
extends ((ShardId, RemoteShardId) => CrossClusterCopyJob[S]) {
  def apply(sourceId: ShardId, destId: RemoteShardId) = {
    new CrossClusterCopyReadJob(sourceId, destId, None, count, ns, s)
  }
}

abstract class CrossClusterCopyJobParser[S <: CopyableShard[_]] extends JsonJobParser {
  protected def cursorFromMap(m: Map[String,Any], key: String) = {
    m.get(key).map(_.asInstanceOf[Map[String,Any]])
  }

  protected def baseDataFromMap(m: Map[String,Any]) = {
    val sourceId = ShardId(m("source_shard_hostname").toString, m("source_shard_table_prefix").toString)
    val destId   = RemoteShardId(
      ShardId(m("destination_shard_hostname").toString, m("destination_shard_table_prefix").toString),
      m("destination_cluster").toString)
    val count    = m("count").asInstanceOf[{def toInt: Int}].toInt

    (sourceId, destId, count)
  }

  def apply(attrs: Map[String,Any]): CrossClusterCopyJob[S]
}


class CrossClusterCopyReadJobParser[S <: CopyableShard[_]](
  ns: NameServer[S],
  s: JobScheduler[JsonJob])
extends CrossClusterCopyJobParser[S] {
  private lazy val writeJobParser = new CrossClusterCopyWriteJobParser(ns,s)

  def apply(attrs: Map[String,Any]) = {
    val (sourceId, destId, count) = baseDataFromMap(attrs)

    val cursor = cursorFromMap(attrs, "cursor")
    val job    = new CrossClusterCopyReadJob[S](sourceId, destId, cursor, count, ns, s)

    job.nextCursorOpt = cursorFromMap(attrs, "next_cursor")
    job.writeJobOpt   = attrs.get("write_job").map(_.asInstanceOf[Map[String,Any]]).map(writeJobParser.parse).map(_.asInstanceOf[CrossClusterCopyWriteJob[S]])

    job
  }
}

class CrossClusterCopyWriteJobParser[S <: CopyableShard[_]](
  ns: NameServer[S],
  s: JobScheduler[JsonJob])
extends CrossClusterCopyJobParser[S] {

  def apply(attrs: Map[String,Any]) = {
    val (sourceId, destId, count) = baseDataFromMap(attrs)

    val pageData   = attrs("data").asInstanceOf[Map[String,Any]]
    val nextCursor = cursorFromMap(attrs, "next_cursor")

    new CrossClusterCopyWriteJob(sourceId, destId, count, pageData, nextCursor, ns, s)
  }
}


abstract class CrossClusterCopyJob[S <: CopyableShard[_]](
  sourceId: ShardId,
  destId: RemoteShardId,
  count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob])
extends JsonJob {
  protected val log = Logger.get(getClass.getName)

  override def shouldReplicate = false

  protected def phase: String
  protected def applyPage(): Unit

  protected def optMap[A](pairs: (String, Option[A])*) = Map(pairs flatMap {
    case (key, opt) => opt.map(key -> _).toList
  }: _*)

  protected def baseMap = Map(
    "source_shard_hostname"          -> sourceId.hostname,
    "source_shard_table_prefix"      -> sourceId.tablePrefix,
    "destination_shard_hostname"     -> destId.id.hostname,
    "destination_shard_table_prefix" -> destId.id.tablePrefix,
    "destination_cluster"            -> destId.cluster,
    "count"                          -> count
  )

  protected def finish() {
    log.info("Cross-cluster copy " + phase + " finished for (type %s) from %s to %s",
             getClass.getName.split("\\.").last, sourceId, destId)
    Stats.clearGauge(gaugeName)
  }

  protected def gaugeName = List("x-cross-cluster", phase, sourceId, destId).mkString("-")

  protected def incrGauge = {
    Stats.setGauge(gaugeName, Stats.getGauge(gaugeName).getOrElse(0.0) + count)
  }

  def apply() {
    try {
      log.info(phase + " shard block (type %s) from %s to relay to %s: state=%s",
               getClass.getName.split("\\.").last, sourceId, destId, toMap)

      applyPage()

    } catch {
      case e: NonExistentShard =>
        log.error("Shard block " + phase + " failed because the source shard doesn't exist. Terminating the copy.")
      case e: ShardDatabaseTimeoutException =>
        log.warning("Shard block " + phase + " failed to get a database connection; retrying.")
        scheduler.put(this)
      case e: Throwable =>
        log.warning("Shard block " + phase + " stopped due to exception: %s", e)
        throw e
    }
  }
}

class CrossClusterCopyWriteJob[S <: CopyableShard[_]](
  sourceId: ShardId,
  destId: RemoteShardId,
  count: Int,
  pageData: Map[String,Any],
  nextCursor: Option[Map[String,Any]],
  ns: NameServer[S],
  s: JobScheduler[JsonJob])
extends CrossClusterCopyJob[S](sourceId, destId, count, ns, s) {

  protected def phase = "write"

  def toMap = baseMap ++ Map("data" -> pageData) ++ optMap("next_cursor" -> nextCursor)

  def applyPage() {
    val dest = ns.findShardById(destId.id)
    dest.writeSerializedPage(pageData)

    if (nextCursor.isEmpty) finish()
  }
}


class CrossClusterCopyReadJob[S <: CopyableShard[_]](
  sourceId: ShardId,
  destId: RemoteShardId,
  cursor: Option[Map[String,Any]],
  var count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob])
extends CrossClusterCopyJob[S](sourceId, destId, count, nameServer, scheduler) {

  protected def phase = "read"

  type Cursor = Map[String,Any]
  type SerializedPage = Map[String,Any]

  var writeJobOpt: Option[CrossClusterCopyWriteJob[S]] = None
  var nextCursorOpt: Option[Cursor] = None

  def writeJob = {
    writeJobOpt getOrElse {
      val source                 = nameServer.findShardById(sourceId)
      val (pageData, nextCursor) = source.readSerializedPage(cursor, count)

      val job = new CrossClusterCopyWriteJob(sourceId, destId, count, pageData, nextCursor, nameServer, scheduler)

      nextCursorOpt = nextCursor
      writeJobOpt   = Some(job)
      job
    }
  }

  def applyPage() {
    try {
      nameServer.jobRelay(destId.cluster)(List(writeJob.toJson))
    } catch {
      case e: ShardTimeoutException if (count > CopyJob.MIN_COPY) =>
        log.warning("Shard block " + phase + " timed out; trying a smaller block size.")
        count = (count * 0.9).toInt
        scheduler.put(this)
    }

    nextCursor match {
      case Some(c) =>
        incrGauge
        scheduler.put(new CrossClusterCopyReadJob(sourceId, destId, Some(c), count, nameServer, scheduler))
      case None =>
        finish()
    }
  }

  def nextCursor = {
    if (nextCursorOpt.isEmpty) writeJob
    nextCursorOpt
  }

  def toMap = {
    baseMap ++ optMap(
      "cursor"      -> cursor,
      "write_job"   -> writeJobOpt.map(_.toMap),
      "next_cursor" -> nextCursorOpt)
  }


}

