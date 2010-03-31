package com.twitter.gizzard.jobs

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.NameServer
import scheduler.JobScheduler
import shards.{Busy, Shard, ShardDatabaseTimeoutException, ShardTimeoutException}


trait CopyShard {
  @throws(classOf[Exception])
  def apply(sourceShard: Shard, destinationShard: Shard, count: Int): Map[String, AnyVal]
}

case class MockCopier(attributes: Map[String, AnyVal]) extends CopyMachine[Shard](attributes) {
  @throws(classOf[Exception])
  protected def copyPage(sourceShard: Shard, destinationShard: Shard, count: Int): Map[String, AnyVal] = {
    copyShard(sourceShard, destinationShard, count)
  }

  var copyIsFinished = false
  var copyShard: CopyShard = null

  protected def finished: Boolean = copyIsFinished
}

object CopyMachineSpec extends Specification with JMocker with ClassMocker {
  "CopyMachine" should {
    val sourceShardId = 1
    val destinationShardId = 2
    val count = CopyMachine.MIN_COPY + 1
    val attributes = CopyMachine.pack(sourceShardId, destinationShardId, count) ++ Map("key" -> 10)
    val copier = new MockCopier(attributes)
    val nameServer = mock[NameServer[Shard]]
    val jobScheduler = mock[JobScheduler]
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]
    copier.copyShard = mock[CopyShard]

    "start" in {
      expect {
        one(jobScheduler).apply(copier)
      }

      copier.start(nameServer, jobScheduler)
    }

    "apply" in {
      val newAttributes = CopyMachine.pack(sourceShardId, destinationShardId, count) ++ Map("key" -> 11)

      "normally" in {
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(copier.copyShard).apply(shard1, shard2, count) willReturn newAttributes
          one(jobScheduler).apply(new MockCopier(newAttributes))
        }

        copier.apply(nameServer, jobScheduler)
      }

      "no shard" in {
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(copier.copyShard).apply(shard1, shard2, count) willThrow new NameServer.NonExistentShard
        }

        copier.apply(nameServer, jobScheduler)
      }

      "with a database connection timeout" in {
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(copier.copyShard).apply(shard1, shard2, count) willThrow new ShardDatabaseTimeoutException
          one(jobScheduler).apply(new MockCopier(attributes ++ Map("count" -> count / 2)))
        }

        copier.apply(nameServer, jobScheduler)
      }

      "with a random exception" in {
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(copier.copyShard).apply(shard1, shard2, count) willThrow new Exception("boo")
        }

        copier.apply(nameServer, jobScheduler) must throwA[Exception]
      }

      "with a shard timeout" in {
        "early on" in {
          val smallerAttributes = attributes ++ Map("count" -> count / 2)

          expect {
            one(nameServer).findShardById(sourceShardId) willReturn shard1
            one(nameServer).findShardById(destinationShardId) willReturn shard2
            one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
            one(copier.copyShard).apply(shard1, shard2, count) willThrow new ShardTimeoutException
            one(jobScheduler).apply(new MockCopier(smallerAttributes))
          }

          copier.apply(nameServer, jobScheduler)
        }

        "after too many retries" in {
          val count = CopyMachine.MIN_COPY - 1
          val attributes = CopyMachine.pack(sourceShardId, destinationShardId, count)
          val copier = new MockCopier(attributes)
          copier.copyShard = mock[CopyShard]

          expect {
            one(nameServer).findShardById(sourceShardId) willReturn shard1
            one(nameServer).findShardById(destinationShardId) willReturn shard2
            one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
            one(copier.copyShard).apply(shard1, shard2, count) willThrow new ShardTimeoutException
          }

          copier.apply(nameServer, jobScheduler) must throwA[Exception]
        }
      }

      "when finished" in {
        expect {
          one(nameServer).findShardById(sourceShardId) willReturn shard1
          one(nameServer).findShardById(destinationShardId) willReturn shard2
          one(nameServer).markShardBusy(destinationShardId, Busy.Busy)
          one(copier.copyShard).apply(shard1, shard2, count) willReturn newAttributes
          one(nameServer).markShardBusy(destinationShardId, Busy.Normal)
        }

        copier.copyIsFinished = true
        copier.apply(nameServer, jobScheduler)
      }
    }
  }
}
