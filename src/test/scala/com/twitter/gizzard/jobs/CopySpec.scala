package com.twitter.gizzard.jobs

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.{NameServer, NonExistentShard}
import scheduler.JobScheduler
import shards._


class FakeCopy(val sourceShardId: ShardId, val destinationShardId: ShardId, count: Int)(nextJob: => Option[FakeCopy]) extends Copy[Shard](sourceShardId, destinationShardId, count) {
  def serialize = Map("cursor" -> 1)

  @throws(classOf[Exception])
  def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int) = nextJob

  override def equals(that: Any) = that match {
    case that: FakeCopy =>
      this.sourceShardId == that.sourceShardId &&
        this.destinationShardId == that.destinationShardId
    case _ => false
  }
}

object CopySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "Copy" should {
    val sourceShardId = ShardId("testhost", "1")
    val destinationShardId = ShardId("testhost", "2")
    val count = Copy.MIN_COPY + 1
    val nextCopy = mock[FakeCopy]
    val makeCopy = new FakeCopy(sourceShardId, destinationShardId, count)(_)
    val nameServer = mock[NameServer[Shard]]
    val jobScheduler = mock[JobScheduler]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]

    "toMap" in {
      val copy = makeCopy(Some(nextCopy))
      copy.toMap mustEqual Map(
        "source_shard_hostname" -> sourceShardId.hostname,
        "source_shard_table_prefix" -> sourceShardId.tablePrefix,
        "destination_shard_hostname" -> destinationShardId.hostname,
        "destination_shard_table_prefix" -> destinationShardId.tablePrefix,
        "count" -> count
      ) ++ copy.serialize
    }

    "apply" in {
      "normally" in {
        val copy = makeCopy(Some(nextCopy))
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(jobScheduler).apply(nextCopy)
        }

        copy.apply(nameServer, jobScheduler)
      }

      "no shard" in {
        val copy = makeCopy(Some(nextCopy))
        expect {
          one(nameServer).findShardById(sourceShardId) willThrow new NonExistentShard
          never(jobScheduler).apply(nextCopy)
        }

        copy.apply(nameServer, jobScheduler)
      }

      "with a database connection timeout" in {
        val copy = makeCopy(throw new ShardDatabaseTimeoutException)
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(jobScheduler).apply(copy)
        }

        copy.apply(nameServer, jobScheduler)
        copy.toMap("count") mustEqual count / 2
      }

      "with a random exception" in {
        val copy = makeCopy(throw new Exception("boo"))
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          never(jobScheduler).apply(nextCopy)
        }

        copy.apply(nameServer, jobScheduler) must throwA[Exception]
      }

      "with a shard timeout" in {
        "early on" in {
          val copy = makeCopy(throw new ShardTimeoutException)
          expect {
            one(nameServer).findShardById(sourceShardId) willReturn shard1
            one(nameServer).findShardById(destinationShardId) willReturn shard2
            one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
            one(jobScheduler).apply(copy)
          }

          copy.apply(nameServer, jobScheduler)
        }

        "after too many retries" in {
          val count = Copy.MIN_COPY - 1
          val copy = new FakeCopy(sourceShardId, destinationShardId, count)(throw new ShardTimeoutException)

          expect {
            one(nameServer).findShardById(sourceShardId) willReturn shard1
            one(nameServer).findShardById(destinationShardId) willReturn shard2
            one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
            never(jobScheduler).apply(nextCopy)
          }

          copy.apply(nameServer, jobScheduler) must throwA[Exception]
        }
      }

      "when finished" in {
        val copy = makeCopy(None)

        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(nameServer).markShardBusy(destinationShardId, Busy.Normal)
        }

        copy.apply(nameServer, jobScheduler)
      }
    }
  }
}
