package com.twitter.gizzard.thrift

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.thrift.conversions.ShardMigration._
import shards.{Busy, Shard}
import scheduler.JobScheduler


object ShardManagerServiceSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val nameServer = mock[nameserver.NameServer[Shard]]
  val copier = mock[jobs.CopyFactory[Shard]]
  val scheduler = mock[JobScheduler]
  val manager = new thrift.ShardManagerService(nameServer, copier, scheduler)
  val shard = mock[Shard]
  val thriftShardInfo1 = new thrift.ShardInfo(new thrift.ShardId("hostname", "table_prefix"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id)
  val shardInfo1 = new shards.ShardInfo(new shards.ShardId("hostname", "table_prefix"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
  val thriftShardInfo2 = new thrift.ShardInfo(new thrift.ShardId("other_table_prefix", "hostname"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id)
  val shardInfo2 = new shards.ShardInfo(new shards.ShardId("other_table_prefix", "hostname"),
    "com.example.SqlShard", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal)
  val hostname = "host1"
  val classname = "com.example.Classname"
  val tableId = 4
  val forwarding = new nameserver.Forwarding(tableId, 0, shardInfo1.id)
  val thriftForwarding = new thrift.Forwarding(tableId, 0, thriftShardInfo1.id)

  "ShardManagerService" should {
    "explode" in {
      expect {
        one(nameServer).createShard(shardInfo1) willThrow new shards.ShardException("blarg!")
      }
      manager.create_shard(thriftShardInfo1) must throwA[thrift.ShardException]
    }

    "create_shard" in {
      expect {
        one(nameServer).createShard(shardInfo1)
      }
      manager.create_shard(thriftShardInfo1)
    }

    "get_shard" in {
      expect {
        one(nameServer).getShard(shardInfo1.id) willReturn shardInfo1
      }
      manager.get_shard(thriftShardInfo1.id) mustEqual thriftShardInfo1
    }

    "delete_shard" in {
      expect {
        one(nameServer).deleteShard(shardInfo1.id)
      }
      manager.delete_shard(thriftShardInfo1.id)
    }

    "add_link" in {
      expect {
        one(nameServer).addLink(shardInfo1.id, shardInfo2.id, 2)
      }
      manager.add_link(thriftShardInfo1.id, thriftShardInfo2.id, 2)
    }

    "remove_link" in {
      expect {
        one(nameServer).removeLink(shardInfo1.id, shardInfo2.id)
      }
      manager.remove_link(thriftShardInfo1.id, thriftShardInfo2.id)
    }

    "list_downward_links" in {
      expect {
        one(nameServer).listDownwardLinks(shardInfo1.id) willReturn List(shards.LinkInfo(shardInfo1.id, shardInfo2.id, 1))
      }
      manager.list_downward_links(thriftShardInfo1.id) mustEqual
        List(new thrift.LinkInfo(thriftShardInfo1.id, thriftShardInfo2.id, 1)).toJavaList
    }

    "mark_shard_busy" in {
      expect {
        one(nameServer).markShardBusy(shardInfo1.id, Busy.Busy)
      }
      manager.mark_shard_busy(thriftShardInfo1.id, Busy.Busy.id)
    }

/*    "copy_shard" in {
      val copyJob = mock[Copy[shards.Shard]]
      val scheduler = mock[JobScheduler]

      expect {
        one(copyManager).newCopyJob(10, 20) willReturn copyJob
        one(copyManager).scheduler willReturn scheduler
        one(copyJob).start(nameServer, scheduler)
      }

      manager.copy_shard(10, 20)
    } */

/*    "setup_migration" in {
      val writeOnlyShard = capturingParam[shards.ShardInfo]
      val replicatingShard = capturingParam[shards.ShardInfo]
      val sourceShardId = 1
      val destinationShardId = 2
      val writeOnlyShardId = 3
      val replicatingShardId = 4

      expect {
        one(nameServer).findShard(thriftShardInfo1.fromThrift) willReturn sourceShardId
        one(nameServer).createShard(thriftShardInfo2.fromThrift) willReturn destinationShardId
        one(nameServer).createShard(writeOnlyShard.capture) willReturn writeOnlyShardId
        one(nameServer).addChildShard(writeOnlyShardId, destinationShardId, 1)
        one(nameServer).createShard(replicatingShard.capture) willReturn replicatingShardId
        one(nameServer).replaceChildShard(sourceShardId, replicatingShardId)
        one(nameServer).addChildShard(replicatingShardId, sourceShardId, 1)
        one(nameServer).addChildShard(replicatingShardId, writeOnlyShardId, 1)
        one(nameServer).replaceForwarding(sourceShardId, replicatingShardId)
      }

      val migration = new thrift.ShardMigration(sourceShardId, destinationShardId, replicatingShardId, writeOnlyShardId)
      manager.setup_migration(thriftShardInfo1, thriftShardInfo2) mustEqual migration
    } */

/*    "migrate shard" in {
      val migration = new ShardMigration(1, 2, 3, 4)
      val migrateJob = mock[Copy[shards.Shard]]
      val scheduler = mock[JobScheduler]

      expect {
        one(copyManager).newMigrateJob(migration.fromThrift) willReturn migrateJob
        one(copyManager).scheduler willReturn scheduler
        one(migrateJob).start(nameServer, scheduler)
      }

      manager.migrate_shard(migration)
    } */

/*    "finish_migration" in {
      val sourceShardId = 1
      val destinationShardId = 2
      val writeOnlyShardId = 3
      val replicatingShardId = 4

      expect {
        one(nameServer).removeChildShard(writeOnlyShardId, destinationShardId)
        one(nameServer).replaceChildShard(replicatingShardId, destinationShardId)
        one(nameServer).replaceForwarding(replicatingShardId, destinationShardId)
        one(nameServer).deleteShard(replicatingShardId)
        one(nameServer).deleteShard(writeOnlyShardId)
        one(nameServer).deleteShard(sourceShardId)
      }

      val migration = new thrift.ShardMigration(sourceShardId, destinationShardId, replicatingShardId, writeOnlyShardId)
      manager.finish_migration(migration)
    } */

    "set_forwarding" in {
      expect {
        one(nameServer).setForwarding(forwarding)
      }
      manager.set_forwarding(thriftForwarding)
    }

    "replace_forwarding" in {
      expect {
        one(nameServer).replaceForwarding(shardInfo1.id, shardInfo2.id)
      }
      manager.replace_forwarding(thriftShardInfo1.id, thriftShardInfo2.id)
    }

    "get_forwarding" in {
      expect {
        one(nameServer).getForwarding(tableId, 0) willReturn forwarding
      }
      manager.get_forwarding(tableId, 0) mustEqual thriftForwarding
    }

    "get_forwarding_for_shard" in {
      expect {
        one(nameServer).getForwardingForShard(shardInfo1.id) willReturn forwarding
      }
      manager.get_forwarding_for_shard(thriftShardInfo1.id) mustEqual thriftForwarding
    }

    "get_forwardings" in {
      expect {
        one(nameServer).getForwardings() willReturn List(forwarding)
      }
      manager.get_forwardings() mustEqual List(thriftForwarding).toJavaList
    }

    "reload_forwardings" in {
      expect {
        one(nameServer).reload()
      }
      manager.reload_forwardings()
    }

    "find_current_forwarding" in {
      expect {
        one(nameServer).findCurrentForwarding(tableId, 23L) willReturn shard
        one(shard).shardInfo willReturn shardInfo1
      }
      manager.find_current_forwarding(tableId, 23L) mustEqual thriftShardInfo1
    }

    "shards_for_hostname" in {
      expect {
        one(nameServer).shardsForHostname(hostname) willReturn List(shardInfo1)
      }
      manager.shards_for_hostname(hostname) mustEqual List(thriftShardInfo1).toJavaList
    }

    "get_busy_shards" in {
      expect {
        one(nameServer).getBusyShards() willReturn List(shardInfo1)
      }
      manager.get_busy_shards() mustEqual List(thriftShardInfo1).toJavaList
    }

    "list_upward_links" in {
      expect {
        one(nameServer).listUpwardLinks(shardInfo1.id) willReturn List(shards.LinkInfo(shardInfo2.id, shardInfo1.id, 1))
      }
      manager.list_upward_links(thriftShardInfo1.id) mustEqual List(new thrift.LinkInfo(thriftShardInfo2.id, thriftShardInfo1.id, 1)).toJavaList
    }

    "get_child_shards_of_class" in {
      expect {
        one(nameServer).getChildShardsOfClass(shardInfo1.id, classname) willReturn List(shardInfo2)
      }
      manager.get_child_shards_of_class(thriftShardInfo1.id, classname) mustEqual List(thriftShardInfo2).toJavaList
    }
  }
}
