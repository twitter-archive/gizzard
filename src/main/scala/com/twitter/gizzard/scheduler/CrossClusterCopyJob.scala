package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import shards._


class CrossClusterCopyJobFactory[S <: Shard](
  ns: NameServer[S],
  s: JobScheduler[JsonJob],
  count: Int,
  shardReader: ((S, Option[Map[String,Any]], Int) => (Map[String,Any], Option[Map[String,Any]])),
  shardWriter: ((S, Map[String,Any]) => Unit))
{
  private def writeJobFactory(sourceId: ShardId, destId: RemoteShardId, data: Map[String,Any], nextCursor: Option[Map[String,Any]], count: Int) = {
    new CrossClusterCopyWriteJob(sourceId, destId, data, nextCursor, count, ns, s, shardWriter)

  }

  private def readJobFactory(sourceId: ShardId, destId: RemoteShardId, cursor: Option[Map[String,Any]], count: Int) = {
    new CrossClusterCopyReadJob(sourceId, destId, cursor, count, ns, s, shardReader, shardWriter)
  }

  def apply(sourceId: ShardId, destId: RemoteShardId) = {
    readJobFactory(sourceId, destId, None, count)
  }

  protected def cursorFromMap(m: Map[String,Any], key: String) = {
    m.get(key).map(_.asInstanceOf[Map[String,Any]])
  }

  protected case class BaseData(
    sourceId: ShardId,
    destId: RemoteShardId,
    count: Int,
    cursor: Option[Map[String,Any]],
    nextCursor: Option[Map[String,Any]]
  )

  protected def baseDataFromMap(m: Map[String,Any]) = {
    val count      = m("count").asInstanceOf[{def toInt: Int}].toInt
    val cursor     = cursorFromMap(m, "cursor")
    val nextCursor = cursorFromMap(m, "next_cursor")
    val sourceId   = ShardId(m("source_shard_hostname").toString, m("source_shard_table_prefix").toString)
    val destId     = RemoteShardId(
      ShardId(m("destination_shard_hostname").toString, m("destination_shard_table_prefix").toString),
      m("destination_cluster").toString)

    BaseData(sourceId, destId, count, cursor, nextCursor)
  }

  lazy val writeParser = new JsonJobParser {
    def apply(attrs: Map[String,Any]) = {
      val data     = baseDataFromMap(attrs)
      val pageData = attrs("data").asInstanceOf[Map[String,Any]]

      writeJobFactory(data.sourceId, data.destId, pageData, data.nextCursor, data.count)
    }
  }

  lazy val readParser = new JsonJobParser {
    def apply(attrs: Map[String,Any]) = {
      val data   = baseDataFromMap(attrs)
      val cursor = data.cursor.get
      val job    = readJobFactory(data.sourceId, data.destId, Some(cursor), data.count)

      job.nextCursorOpt = data.nextCursor
      job.writeJobOpt   = attrs.get("write_job").map(_.asInstanceOf[Map[String,Any]]).map(writeParser.parse).map(_.asInstanceOf[CrossClusterCopyWriteJob[S]])

      job
    }
  }
}


abstract class CrossClusterCopyJob[S <: Shard](
  sourceId: ShardId,
  destId: RemoteShardId,
  count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob])
extends JsonJob {

  protected def phase: String
  protected def applyPage(): Unit

  protected val log = Logger.get(getClass.getName)

  override def shouldReplicate = false

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

  protected def gaugeName = Array("x-cross-cluster", phase, sourceId, destId).mkString("-")

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

class CrossClusterCopyReadJob[S <: Shard](
  sourceId: ShardId,
  destId: RemoteShardId,
  cursor: Option[Map[String,Any]],
  var count: Int,
  nameServer: NameServer[S],
  scheduler: JobScheduler[JsonJob],
  readData: ((S, Option[Map[String,Any]], Int) => (Map[String,Any], Option[Map[String,Any]])),
  writeData: ((S, Map[String,Any]) => Unit))
extends CrossClusterCopyJob[S](sourceId, destId, count, nameServer, scheduler) {

  protected def phase = "read"

  private def newWriteJob(data: Map[String,Any], nextCursor: Option[Map[String,Any]]) = {
    new CrossClusterCopyWriteJob(sourceId, destId, data, nextCursor, count, nameServer, scheduler, writeData)
  }

  private def newReadJob(cursor: Map[String,Any]): CrossClusterCopyReadJob[S] = {
    new CrossClusterCopyReadJob(sourceId, destId, Some(cursor), count, nameServer, scheduler, readData, writeData)
  }

  var writeJobOpt: Option[CrossClusterCopyWriteJob[S]] = None
  var nextCursorOpt: Option[Map[String,Any]] = None

  def writeJob = {
    writeJobOpt getOrElse {
      val source                 = nameServer.findShardById(sourceId)
      val (pageData, nextCursor) = readData(source, cursor, count)

      val job = newWriteJob(pageData, nextCursor)

      nextCursorOpt = nextCursor
      writeJobOpt   = Some(job)
      job
    }
  }

  def nextCursor = {
    if (nextCursorOpt.isEmpty) writeJob
    nextCursorOpt
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
        scheduler.put(newReadJob(c))
      case None =>
        finish()
    }
  }

  def toMap = {
    baseMap ++ optMap(
      "cursor"      -> cursor,
      "write_job"   -> writeJobOpt.map(_.toMap),
      "next_cursor" -> nextCursorOpt)
  }
}


class CrossClusterCopyWriteJob[S <: Shard](
  sourceId: ShardId,
  destId: RemoteShardId,
  pageData: Map[String,Any],
  nextCursor: Option[Map[String,Any]],
  count: Int,
  ns: NameServer[S],
  s: JobScheduler[JsonJob],
  writeData: ((S, Map[String,Any]) => Unit))
extends CrossClusterCopyJob[S](sourceId, destId, count, ns, s) {

  protected def phase = "write"
  def toMap = baseMap ++ Map("data" -> pageData) ++ optMap("next_cursor" -> nextCursor)

  protected def applyPage() {
    val dest = ns.findShardById(destId.id)
    writeData(dest, pageData)

    if (nextCursor.isEmpty) finish()
  }
}





/*
 * Concrete cross-cluster copy support for copyable shards
 */

class BasicCrossClusterCopy[S <: CopyableShard[_]] {
  def shardReader(shard: S, cursor: Option[Map[String,Any]], count: Int) = {
    shard.readSerializedPage(cursor, count)
  }

  def shardWriter(shard: S, data: Map[String,Any]) {
    shard.writeSerializedPage(data)
  }
}


