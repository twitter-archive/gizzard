package com.twitter.gizzard.jobs

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import nameserver.ShardMigration
import scheduler.JobScheduler
import shards.{Busy, Shard, ShardDatabaseTimeoutException, ShardTimeoutException}


object MigrateSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  /*"Migrate" should {
    val copyJob = mock[Copy[Shard]]
    val sourceShardId = 20
    val destinationShardId = 25
    val replicatingShardId = 29
    val writeOnlyShardId = 28
    val migration = new ShardMigration(sourceShardId, destinationShardId, replicatingShardId,
      writeOnlyShardId)
    val shard1 = mock[Shard]
    val shard2 = mock[Shard]

    var migrate: Migrate[Shard] = null

    doBefore {
      expect {
        allowing(copyJob).count willReturn 100
      }
      migrate = new Migrate[Shard](copyJob, migration)
    }

    "serialize" in {
      expect {
        one(copyJob).serialize() willReturn Map.empty[String, AnyVal]
      }
      migrate.serialize
    }

    "copyPage" in {
      val copyJob2 = mock[Copy[Shard]]
      expect {
        one(copyJob).copyPage(shard1, shard2, 20) willReturn Some(copyJob2)
        one(copyJob2).count willReturn 100
      }
      migrate.copyPage(shard1, shard2, 20) must beSome[Migrate[Shard]].which { m => m.copy mustEqual copyJob2 }
    }
  } */
}
