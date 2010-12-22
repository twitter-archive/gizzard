package com.twitter.gizzard.scheduler

import com.twitter.util.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import shards._

class FakeCopyAdapter(maxCount: Int) extends ShardCopyAdapter[Shard] {
  var pagesWritten = 0

  def readPage(source: Shard, cursor: Option[Map[String,Any]], count: Int) = {
    val lastCount  = cursor.flatMap(_.get("last_count")).map(_.asInstanceOf[{ def toInt: Int}].toInt) getOrElse 0
    val nextCursor = if (lastCount < maxCount) Some(Map("last_count" -> (lastCount + count))) else None
    println(nextCursor)
    println(count)
    (Map("count" -> (lastCount + count)), nextCursor)
  }

  def writePage(dest: Shard, data: Map[String,Any]) {
    pagesWritten += 1
  }

  def copyPage(source: Shard, dest: Shard, cursor: Option[Map[String,Any]], count: Int) = {
    val (data, nextCursor) = readPage(source, cursor, count)
    writePage(dest, data)
    nextCursor
  }
}

object BasicCopyJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "BasicCopyJob" should {
    val sourceId = ShardId("testhost", "1")
    val destinationId = ShardId("testhost", "2")
    val count = 1
    val nameServer = mock[nameserver.NameServer[Shard]]
    val jobScheduler = mock[JobScheduler[JsonJob]]
    val shardCopyAdapter = new FakeCopyAdapter(10)
    val copyFactory = new BasicCopyJobFactory(nameServer, jobScheduler, shardCopyAdapter, count)
    val codec = new JsonCodec({ a => error(new String(a, "UTF-8")) })
    codec += ("BasicCopyJob".r -> copyFactory.parser)
    val source = mock[Shard]
    val destination = mock[Shard]

    "toMap" in {
      val copy = copyFactory(sourceId, destinationId)
      copy.toMap mustEqual Map(
        "source_shard_hostname" -> sourceId.hostname,
        "source_shard_table_prefix" -> sourceId.tablePrefix,
        "destination_shard_hostname" -> destinationId.hostname,
        "destination_shard_table_prefix" -> destinationId.tablePrefix,
        "count" -> count
      ) ++ copy.serialize
    }

    "toJson" in {
      val copy = copyFactory(sourceId, destinationId)
      val json = copy.toJson
      json mustMatch "Copy"
      json mustMatch "\"source_shard_hostname\":\"%s\"".format(sourceId.hostname)
      json mustMatch "\"source_shard_table_prefix\":\"%s\"".format(sourceId.tablePrefix)
      json mustMatch "\"destination_shard_hostname\":\"%s\"".format(destinationId.hostname)
      json mustMatch "\"destination_shard_table_prefix\":\"%s\"".format(destinationId.tablePrefix)
      json mustMatch "\"count\":" + count
    }

    "parse" in {
      val copy       = copyFactory(sourceId, destinationId)
      val json       = copy.toJson
      val parsedCopy = codec.inflate(json.getBytes("UTF-8"))

      parsedCopy.toJson mustEqual json
    }

    "multiple page apply" in {
      val copy = copyFactory(sourceId, destinationId)

      expect {
        one(nameServer).markShardBusy(destinationId, Busy.Busy)
        one(nameServer).findShardById(sourceId) willReturn source
        one(nameServer).findShardById(destinationId) willReturn destination
        one(nameServer).markShardBusy(destinationId, Busy.Normal)
      }

      copy.apply()

      println(shardCopyAdapter.pagesWritten)
    }

    "single page apply" in {
      val shardCopyAdapter = new FakeCopyAdapter(1)
      val copyFactory = new BasicCopyJobFactory(nameServer, jobScheduler, shardCopyAdapter, count)
      val copy = copyFactory(sourceId, destinationId)

      expect {
        one(nameServer).markShardBusy(destinationId, Busy.Busy)
        one(nameServer).findShardById(sourceId) willReturn source
        one(nameServer).findShardById(destinationId) willReturn destination
        val nextJobP = capturingParam[JsonJob]
        one(jobScheduler).put(nextJobP.capture)
      }

      copy.apply()

      println(shardCopyAdapter.pagesWritten)
    }
  }
}
