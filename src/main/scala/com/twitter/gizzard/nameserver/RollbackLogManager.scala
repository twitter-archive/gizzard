package com.twitter.gizzard.nameserver

import java.util.UUID
import java.nio.ByteBuffer
import scala.collection.mutable
import scala.math.Ordering

import com.twitter.logging.Logger
import com.twitter.gizzard.shards.RoutingNode
import com.twitter.gizzard.thrift


class RollbackLogManager(shard: RoutingNode[ShardManagerSource]) {
  import RollbackLogManager._

  def create(logName: String): ByteBuffer = {
    val idArray = new Array[Byte](UUID_BYTES)
    val id = ByteBuffer.wrap(idArray)
    val uuid = UUID.randomUUID
    id.asLongBuffer
      .put(uuid.getMostSignificantBits)
      .put(uuid.getLeastSignificantBits)
    shard.write.foreach(_.logCreate(idArray, logName))
    log.info("Created new rollback log for name '%s' with id %s", logName, uuid)
    id
  }

  def get(logName: String): Option[ByteBuffer] =
    shard.read.any(_.logGet(logName).map(ByteBuffer.wrap))

  def entryPush(logId: ByteBuffer, entry: thrift.LogEntry): Unit =
    shard.write.foreach(_.logEntryPush(unwrap(logId), entry))

  /**
   * @return the entries with the highest ids on all replicas: note that this means
   * that logs cannot be rolled back while any nameserver replicas are unavailable.
   */
  def entryPeek(logId: ByteBuffer, count: Int): Seq[thrift.LogEntry] = {
    // collect k from each shard
    val descendingEntryLists = shard.read.map(_.logEntryPeek(unwrap(logId), count))
    // and take the top k
    RollbackLogManager.topK(descendingEntryLists, count)
  }

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
  private val log = Logger.get(getClass.getName)

  val UUID_BYTES = 16

  /**
   * Calculates the top-k entries by id.
   * TODO: this is not the hot path, but we should eventually do a real linear merge.
   */
  def topK(
    descendingEntryLists: Iterable[Seq[thrift.LogEntry]],
    count: Int
  ): Seq[thrift.LogEntry] = count match {
    case 1 =>
      val heads = descendingEntryLists.flatMap(_.lastOption)
      // no optional form of max
      if (!heads.isEmpty)
        Seq(heads.max(EntryOrdering))
      else
        Nil
    case _ =>
      val q = new mutable.PriorityQueue[thrift.LogEntry]()(EntryOrdering)
      val result = mutable.Buffer[thrift.LogEntry]()
      descendingEntryLists.foreach { q ++= _ }
      val iterator = q.iterator
      // take the first k non-equal entries
      var gathered = 0
      var lastId = Int.MinValue
      while (gathered < count && iterator.hasNext) {
        val entry = iterator.next
        if (entry.id != lastId) {
          result += entry
          lastId = entry.id
          gathered += 1
        }
      }
      result
  }

  // TODO: Scala 2.8.1 doesn't have maxBy
  object EntryOrdering extends Ordering[thrift.LogEntry] {
    override def compare(a: thrift.LogEntry, b: thrift.LogEntry) =
      Ordering.Int.compare(a.id, b.id)
  }
}
