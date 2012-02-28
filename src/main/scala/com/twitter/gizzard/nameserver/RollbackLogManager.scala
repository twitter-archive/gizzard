package com.twitter.gizzard.nameserver

import java.util.UUID
import java.nio.ByteBuffer

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

  def entryPeek(logId: ByteBuffer): Option[thrift.LogEntry] =
    shard.read.any(_.logEntryPeek(unwrap(logId)))

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
}
