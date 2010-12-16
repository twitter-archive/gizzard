package com.twitter.gizzard.scheduler

import com.twitter.ostrich.Stats
import com.twitter.util.TimeConversions._
import net.lag.logging.Logger
import nameserver.{NameServer, NonExistentShard}
import shards._


class CrossClusterCopyJobFactory[S <: Shard](
  val nameServer: NameServer[S],
  val scheduler: JobScheduler[JsonJob],
  val copyAdapter: ShardCopyAdapter[S],
  val defaultCount: Int)
extends ((ShardId, RemoteShardId) => JsonJob) {

  private def factory = this

  def apply(sourceId: ShardId, destId: RemoteShardId) = {
    new CrossClusterCopyReadJob(sourceId, destId, None, defaultCount, this)
  }

  protected def getAs[A](m: Map[String,Any], k: String) = {
    m.get(k).map(_.asInstanceOf[A])
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
    val cursor     = getAs[Map[String,Any]](m, "cursor")
    val nextCursor = getAs[Map[String,Any]](m, "cursor")
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

      new CrossClusterCopyWriteJob(data.sourceId, data.destId, pageData, data.nextCursor, data.count, factory)
    }
  }

  lazy val readParser = new JsonJobParser {
    def apply(attrs: Map[String,Any]) = {
      val data = baseDataFromMap(attrs)
      val job  = new CrossClusterCopyReadJob(data.sourceId, data.destId, data.cursor, data.count, factory)

      job.nextCursorOpt = data.nextCursor
      job.writeJobOpt   = attrs.get("write_job").map { m =>
        val j = writeParser.parse(m.asInstanceOf[Map[String,Any]])
        j.asInstanceOf[CrossClusterCopyWriteJob[S]]
      }

      job
    }
  }
}


abstract class CrossClusterCopyJob[S <: Shard](
  sourceId: ShardId,
  destId: RemoteShardId,
  count: Int,
  factory: CrossClusterCopyJobFactory[S])
extends JsonJob {

  protected def phase: String
  protected def applyPage(): Unit

  protected val log = Logger.get(getClass.getName)

  override def shouldReplicate = false

  protected def nameServer = factory.nameServer
  protected def scheduler  = factory.scheduler
  protected def readPage   = factory.copyAdapter.readPage _
  protected def writePage  = factory.copyAdapter.writePage _
  protected def copyPage   = factory.copyAdapter.copyPage _


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
  factory: CrossClusterCopyJobFactory[S])
extends CrossClusterCopyJob[S](sourceId, destId, count, factory) {

  protected def phase = "read"

  private def newWriteJob(data: Map[String,Any], nextCursor: Option[Map[String,Any]]) = {
  }

  private def newReadJob(cursor: Map[String,Any]): CrossClusterCopyReadJob[S] = {
    new CrossClusterCopyReadJob(sourceId, destId, Some(cursor), count, factory)
  }

  var writeJobOpt: Option[CrossClusterCopyWriteJob[S]] = None
  var nextCursorOpt: Option[Map[String,Any]] = None

  def writeJob = writeJobOpt getOrElse {
    val source                 = nameServer.findShardById(sourceId)
    val (pageData, nextCursor) = readPage(source, cursor, count)

    val job = new CrossClusterCopyWriteJob(
      sourceId,
      destId,
      pageData,
      nextCursor,
      count,
      factory)

    nextCursorOpt = nextCursor
    writeJobOpt   = Some(job)
    job
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
        scheduler.put(new CrossClusterCopyReadJob(
          sourceId,
          destId,
          Some(c),
          count,
          factory))
      case None =>
        finish()
    }
  }

  def toMap = baseMap ++ optMap(
    "cursor"      -> cursor,
    "write_job"   -> writeJobOpt.map(_.toMap),
    "next_cursor" -> nextCursorOpt
  )
}


class CrossClusterCopyWriteJob[S <: Shard](
  sourceId: ShardId,
  destId: RemoteShardId,
  pageData: Map[String,Any],
  nextCursor: Option[Map[String,Any]],
  count: Int,
  factory: CrossClusterCopyJobFactory[S])
extends CrossClusterCopyJob[S](sourceId, destId, count, factory) {

  protected def phase = "write"
  def toMap = baseMap ++ Map("data" -> pageData) ++ optMap("next_cursor" -> nextCursor)

  protected def applyPage() {
    val dest = nameServer.findShardById(destId.id)
    writePage(dest, pageData)

    if (nextCursor.isEmpty) finish()
  }
}
