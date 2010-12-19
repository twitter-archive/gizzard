package com.twitter.gizzard.scheduler

import com.twitter.util.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class FakeCopy(val sourceShardId: shards.ShardId, val dests: List[CopyDestination], count: Int,
               nameServer: nameserver.NameServer[shards.Shard], scheduler: JobScheduler[JsonJob])(nextJob: => Option[FakeCopy])
      extends CopyJob[shards.Shard](sourceShardId, dests, count, nameServer, scheduler) {
  def serialize = Map("cursor" -> 1)

  @throws(classOf[Exception])
  def copyPage(sourceShard: shards.Shard, destinationShards: List[CopyDestinationShard[shards.Shard]], count: Int) = {
    nextJob
  }

  override def equals(that: Any) = that match {
    case that: FakeCopy =>
      this.sourceShardId == that.sourceShardId
    case _ => false
  }
}

object CopyJobSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "CopyJob" should {
    val sourceShardId = shards.ShardId("testhost", "1")
    val destinationShardId = shards.ShardId("testhost", "2")
    val destinations = List(CopyDestination(destinationShardId, None))
    val count = CopyJob.MIN_COPY + 1
    val nextCopy = mock[FakeCopy]
    val nameServer = mock[nameserver.NameServer[shards.Shard]]
    val jobScheduler = mock[JobScheduler[JsonJob]]
    val makeCopy = new FakeCopy(sourceShardId, destinations, count, nameServer, jobScheduler)(_)
    val shard1 = mock[shards.Shard]
    val shard2 = mock[shards.Shard]

    "toMap" in {
      val copy = makeCopy(Some(nextCopy))
      copy.toMap mustEqual Map(
        "source_shard_hostname" -> sourceShardId.hostname,
        "source_shard_table_prefix" -> sourceShardId.tablePrefix,
        "destination_0_hostname" -> destinationShardId.hostname,
        "destination_0_table_prefix" -> destinationShardId.tablePrefix,
        "count" -> count
      ) ++ copy.serialize
    }

    "toJson" in {
      val copy = makeCopy(Some(nextCopy))
      val json = copy.toJson
      json mustMatch "Copy"
      json mustMatch "\"source_shard_hostname\":\"%s\"".format(sourceShardId.hostname)
      json mustMatch "\"source_shard_table_prefix\":\"%s\"".format(sourceShardId.tablePrefix)
      json mustMatch "\"destination_0_hostname\":\"%s\"".format(destinationShardId.hostname)
      json mustMatch "\"destination_0_table_prefix\":\"%s\"".format(destinationShardId.tablePrefix)
      json mustMatch "\"count\":" + count
    }

    "apply" in {
      "normally" in {
        val copy = makeCopy(Some(nextCopy))
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, shards.Busy.Busy)
          one(jobScheduler).put(nextCopy)
        }

        copy.apply()
      }

      "no shard" in {
        val copy = makeCopy(Some(nextCopy))
        expect {
          one(nameServer).findShardById(sourceShardId) willThrow new nameserver.NonExistentShard("foo")
          never(jobScheduler).put(nextCopy)
        }

        copy.apply()
      }

      "with a database connection timeout" in {
         val copy = makeCopy(throw new shards.ShardDatabaseTimeoutException(100.milliseconds, sourceShardId))
         expect {
           one(nameServer).findShardById(sourceShardId) willReturn shard1
           one(nameServer).findShardById(destinationShardId) willReturn shard2
           one(nameServer).markShardBusy(destinationShardId, shards.Busy.Busy)
           one(jobScheduler).put(copy)
         }

         copy.apply()
         copy.toMap("count") mustEqual (count * 0.9).toInt
       }

      "with a random exception" in {
        val copy = makeCopy(throw new Exception("boo"))
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, shards.Busy.Busy)
          never(jobScheduler).put(nextCopy)
        }

        copy.apply() must throwA[Exception]
      }

      "with a shard timeout" in {
        "early on" in {
          val copy = makeCopy(throw new shards.ShardTimeoutException(100.milliseconds, sourceShardId))
          expect {
            one(nameServer).findShardById(sourceShardId) willReturn shard1
            one(nameServer).findShardById(destinationShardId) willReturn shard2
            one(nameServer).markShardBusy(destinationShardId, shards.Busy.Busy)
            one(jobScheduler).put(copy)
          }

          copy.apply()
        }

        "after too many retries" in {
          val count = CopyJob.MIN_COPY - 1
          val copy = new FakeCopy(sourceShardId, destinations, count, nameServer, jobScheduler)(throw new shards.ShardTimeoutException(100.milliseconds, sourceShardId))

          expect {
            one(nameServer).findShardById(sourceShardId) willReturn shard1
            one(nameServer).findShardById(destinationShardId) willReturn shard2
            one(nameServer).markShardBusy(destinationShardId, shards.Busy.Busy)
            never(jobScheduler).put(nextCopy)
          }

          copy.apply() must throwA[Exception]
        }
      }

      "when finished" in {
        val copy = makeCopy(None)

        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, shards.Busy.Busy)
          one(nameServer).markShardBusy(destinationShardId, shards.Busy.Normal)
        }

        copy.apply()
      }
    }
  }
}
