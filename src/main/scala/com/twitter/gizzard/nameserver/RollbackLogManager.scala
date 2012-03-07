package com.twitter.gizzard.nameserver

import java.util.UUID
import java.nio.ByteBuffer
import scala.math.Ordering

import com.twitter.gizzard.shards.RoutingNode
import com.twitter.gizzard.thrift


class RollbackLogManager(shard: RoutingNode[ShardManagerSource]) {
  def create(logName: String): ByteBuffer = {
    val idArray = new Array[Byte](RollbackLogManager.UUID_BYTES)
    val id = ByteBuffer.wrap(idArray)
    val uuid = UUID.randomUUID
    id.asLongBuffer
      .put(uuid.getMostSignificantBits)
      .put(uuid.getLeastSignificantBits)
    shard.write.foreach(_.logCreate(idArray, logName))
    id
  }

  def get(logName: String): Option[ByteBuffer] =
    shard.read.any(_.logGet(logName).map(ByteBuffer.wrap))

  def entryPush(logId: ByteBuffer, entry: thrift.LogEntry): Unit =
    shard.write.foreach(_.logEntryPush(unwrap(logId), entry))

  /**
   * @return the entry with the highest id on all replicas: note that this means
   * that logs cannot be rolled back while any nameserver replicas are unavailable.
   */
  def entryPeek(logId: ByteBuffer): Option[thrift.LogEntry] =
    shard.read.map(_.logEntryPeek(unwrap(logId))).max(RollbackLogManager.EntryOrdering)

  def entryPop(logId: ByteBuffer, entryId: Int): Unit =
    shard.write.foreach(_.logEntryPop(unwrap(logId), entryId))

  private def unwrap(bb: ByteBuffer): Array[Byte] = {
    if (bb.hasArray && bb.remaining == bb.capacity) {
      bb.array
    } else {
      val arr = new Array[Byte](bb.remaining)
      bb.duplicate.get(arr)
      arr
    }
  }
}

object RollbackLogManager {
  val UUID_BYTES = 16
  // TODO: Scala 2.8.1 doesn't have maxBy
  object EntryOrdering extends Ordering[Option[thrift.LogEntry]] {
    override def compare(a: Option[thrift.LogEntry], b: Option[thrift.LogEntry]) =
      Ordering.Int.compare(
        a.map(_.id).getOrElse(Int.MinValue),
        b.map(_.id).getOrElse(Int.MinValue)
      )
  }
}
