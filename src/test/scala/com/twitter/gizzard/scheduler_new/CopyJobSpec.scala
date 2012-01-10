package com.twitter.gizzard.scheduler

import com.twitter.conversions.time._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

import com.twitter.gizzard.nameserver._
import com.twitter.gizzard.shards._
import com.twitter.gizzard.ConfiguredSpecification


class FakeCopy(
  shardIds: Seq[ShardId],
  count: Int,
  nameServer: NameServer,
  scheduler: JobScheduler,
  nextCopy: => Option[FakeCopy])
extends CopyJob[AnyRef](shardIds, count, nameServer, scheduler) {

  def serialize = Map("cursor" -> 1)

  @throws(classOf[Exception])
  def copyPage(nodes: Seq[RoutingNode[AnyRef]], count: Int) = nextCopy

  override def equals(that: Any) = that match {
    case that: FakeCopy =>
      this.shardIds.toList == that.shardIds.toList
    case _ => false
  }
}

//TODO: Add multi-shard copy/repair test
object CopyJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "CopyJob" should {
    val shardId1 = ShardId("testhost", "1")
    val shardId2 = ShardId("testhost", "2")
    val shardIds = Seq(shardId1, shardId2)
    val shard1Info = ShardInfo(shardId1, "FakeShard", "", "", Busy.Normal)
    val shard2Info = ShardInfo(shardId2, "FakeShard", "", "", Busy.Normal)
    val count = CopyJob.MIN_COPY + 1
    val nextCopy = mock[FakeCopy]
    val nameServer = mock[NameServer]
    val shardManager = mock[ShardManager]
    val jobScheduler = mock[JobScheduler]
    def makeCopy(next: => Option[FakeCopy]) = new FakeCopy(shardIds, count, nameServer, jobScheduler, next)
    val shard1 = mock[RoutingNode[AnyRef]]
    val shard2 = mock[RoutingNode[AnyRef]]
t

    expect {
      allowing(nameServer).shardManager willReturn shardManager
    }

    "toMap" in {
      val copy = makeCopy(Some(nextCopy))
      copy.toMap mustEqual Map(
        "shards" -> Seq(Map("hostname" -> "testhost", "table_prefix" -> "1"), Map("hostname" -> "testhost", "table_prefix" -> "2")),
        "count" -> count
      ) ++ copy.serialize
    }

    "toJson" in {
      val copy = makeCopy(Some(nextCopy))
      val json = copy.toJson
      json mustMatch "Copy"
      json mustMatch "\"hostname\":\"%s\"".format(shardId1.hostname)
      json mustMatch "\"table_prefix\":\"%s\"".format(shardId1.tablePrefix)
      json mustMatch "\"hostname\":\"%s\"".format(shardId2.hostname)
      json mustMatch "\"table_prefix\":\"%s\"".format(shardId2.tablePrefix)
      json mustMatch "\"count\":" + count
    }

    "apply" in {
      "normally" in {
        val copy = makeCopy(Some(nextCopy))
        expect {
          one(shardManager).getShard(shardId1) willReturn shard1Info
          one(shardManager).getShard(shardId2) willReturn shard2Info
          one(nameServer).findShardById(shardId1) willReturn shard1
          one(nameServer).findShardById(shardId2) willReturn shard2
          one(shardManager).markShardBusy(shardId1, Busy.Busy)
          one(shardManager).markShardBusy(shardId2, Busy.Busy)
        }

        copy.apply()
      }

      "missing shard" in {
        val copy = makeCopy(Some(nextCopy))
        expect {
          one(shardManager).getShard(shardId1) willReturn shard1Info
          one(shardManager).getShard(shardId2) willThrow new NonExistentShard("foo")
        }

        copy.apply()
      }

      "with a database connection timeout" in {
        val copy = makeCopy(throw new ShardDatabaseTimeoutException(100.milliseconds, shardId1))
        expect {
          one(shardManager).getShard(shardId1) willReturn shard1Info
          one(shardManager).getShard(shardId2) willReturn shard2Info
          one(nameServer).findShardById(shardId1) willReturn shard1
          one(nameServer).findShardById(shardId2) willReturn shard2
          one(shardManager).markShardBusy(shardId1, Busy.Busy)
          one(shardManager).markShardBusy(shardId2, Busy.Busy)
          one(jobScheduler).put(copy)
        }

        copy.apply()
        copy.toMap("count") mustEqual (count * 0.9).toInt
      }

      "with a random exception" in {
        val copy = makeCopy(throw new Exception("boo"))
        expect {
          one(shardManager).getShard(shardId1) willReturn shard1Info
          one(shardManager).getShard(shardId2) willReturn shard2Info
          one(nameServer).findShardById(shardId1) willReturn shard1
          one(nameServer).findShardById(shardId2) willReturn shard2
          one(shardManager).markShardBusy(shardId1, Busy.Busy)
          one(shardManager).markShardBusy(shardId2, Busy.Busy)
          one(shardManager).markShardBusy(shardId1, Busy.Error)
          one(shardManager).markShardBusy(shardId2, Busy.Error)
          never(jobScheduler).put(nextCopy)
        }

        copy.apply() must throwA[Exception]
      }

      "with a shard timeout" in {
        "early on" in {
          val copy = makeCopy(throw new ShardTimeoutException(100.milliseconds, shardId1))
          expect {
            one(shardManager).getShard(shardId1) willReturn shard1Info
            one(shardManager).getShard(shardId2) willReturn shard2Info
            one(nameServer).findShardById(shardId1) willReturn shard1
            one(nameServer).findShardById(shardId2) willReturn shard2
            one(shardManager).markShardBusy(shardId1, Busy.Busy)
            one(shardManager).markShardBusy(shardId2, Busy.Busy)
            one(jobScheduler).put(copy)
          }

          copy.apply()
        }

        "after too many retries" in {
          val count = CopyJob.MIN_COPY - 1
          val copy = new FakeCopy(shardIds, count, nameServer, jobScheduler, throw new ShardTimeoutException(100.milliseconds, shardId1))

          expect {
            one(shardManager).getShard(shardId1) willReturn shard1Info
            one(shardManager).getShard(shardId2) willReturn shard2Info
            one(nameServer).findShardById(shardId1) willReturn shard1
            one(nameServer).findShardById(shardId2) willReturn shard2
            one(shardManager).markShardBusy(shardId1, Busy.Busy)
            one(shardManager).markShardBusy(shardId2, Busy.Busy)
            one(shardManager).markShardBusy(shardId1, Busy.Error)
            one(shardManager).markShardBusy(shardId2, Busy.Error)
            never(jobScheduler).put(nextCopy)
          }

          copy.apply() must throwA[Exception]
        }
      }

      "when cancelled" in {
        val copy = makeCopy(Some(nextCopy))
        val cancelledInfo = ShardInfo(shardId2, "FakeShard", "", "", Busy.Cancelled)

        expect {
          one(shardManager).getShard(shardId1) willReturn shard1Info
          one(shardManager).getShard(shardId2) willReturn cancelledInfo
          never(nameServer).findShardById(shardId1)
          never(nameServer).findShardById(shardId2)
          never(shardManager).markShardBusy(shardId1, Busy.Busy)
          never(shardManager).markShardBusy(shardId2, Busy.Busy)
          never(jobScheduler).put(nextCopy)
        }

        copy.apply()
      }

      "when finished" in {
        val copy = makeCopy(None)

        expect {
          one(shardManager).getShard(shardId1) willReturn shard1Info
          one(shardManager).getShard(shardId2) willReturn shard2Info
          one(nameServer).findShardById(shardId1) willReturn shard1
          one(nameServer).findShardById(shardId2) willReturn shard2
          one(shardManager).markShardBusy(shardId1, Busy.Busy)
          one(shardManager).markShardBusy(shardId2, Busy.Busy)
          one(shardManager).markShardBusy(shardId2, Busy.Normal)
          one(shardManager).markShardBusy(shardId1, Busy.Normal)
        }

        copy.apply()
      }
    }
  }
}
