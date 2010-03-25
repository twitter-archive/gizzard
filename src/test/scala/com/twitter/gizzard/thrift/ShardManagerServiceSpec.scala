package com.twitter.gizzard.thrift

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.thrift.conversions.Sequences._
import com.twitter.gizzard.thrift.conversions.ShardInfo._
import com.twitter.gizzard.thrift.conversions.ShardMigration._
import shards.Busy
import scheduler.JobScheduler


object ShardManagerServiceSpec extends Specification with JMocker with ClassMocker {
  val nameServer = mock[nameserver.CachingNameServer]
  val copyMachine = mock[nameserver.CopyManager]
  val manager = new thrift.ShardManagerService(nameServer, copyMachine)
  val thriftShardInfo1 = new thrift.ShardInfo("com.example.SqlShard",
    "table_prefix", "hostname", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id, 1)
  val shardInfo1 = new shards.ShardInfo("com.example.SqlShard",
    "table_prefix", "hostname", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal, 1)
  val thriftShardInfo2 = new thrift.ShardInfo("com.example.SqlShard",
    "other_table_prefix", "hostname", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id, 1)
  val shardInfo2 = new shards.ShardInfo("com.example.SqlShard",
    "other_table_prefix", "hostname", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal, 1)
  val hostname = "host1"
  val classname = "com.example.Classname"
  val shardId = 100
  val parentShardId = 900
  val childShardId1 = 200
  val childShardId2 = 201
  val tableId = 4
  val forwarding = new nameserver.Forwarding(tableId, 0, shardId)
  val thriftForwarding = new thrift.Forwarding(tableId, 0, shardId)

  "ShardManagerService" should {
    "create_shard" in {
      expect {
        one(nameServer).createShard(shardInfo1) willReturn shardId
      }
      manager.create_shard(thriftShardInfo1) mustEqual shardId
    }

    "find_shard" in {
      expect {
        one(nameServer).findShard(shardInfo1) willReturn shardId
      }
      manager.find_shard(thriftShardInfo1) mustEqual shardId
    }

    "get_shard" in {
      expect {
        one(nameServer).getShard(shardId) willReturn shardInfo1
      }
      manager.get_shard(shardId) mustEqual thriftShardInfo1
    }

    "update_shard" in {
      expect {
        one(nameServer).updateShard(shardInfo1)
      }
      manager.update_shard(thriftShardInfo1)
    }

    "delete_shard" in {
      expect {
        one(nameServer).deleteShard(shardId)
      }
      manager.delete_shard(shardId)
    }

    "add_child_shard" in {
      expect {
        one(nameServer).addChildShard(shardId, childShardId1, 2)
      }
      manager.add_child_shard(shardId, childShardId1, 2)
    }

    "remove_child_shard" in {
      expect {
        one(nameServer).removeChildShard(shardId, childShardId1)
      }
      manager.remove_child_shard(shardId, childShardId1)
    }

    "replace_child_shard" in {
      expect {
        one(nameServer).replaceChildShard(childShardId1, childShardId2)
      }
      manager.replace_child_shard(childShardId1, childShardId2)
    }

    "list_shard_children" in {
      expect {
        one(nameServer).listShardChildren(shardId) willReturn List(new shards.ChildInfo(childShardId1, 1), new shards.ChildInfo(childShardId2, 1))
      }
      manager.list_shard_children(shardId) mustEqual
        List(new thrift.ChildInfo(childShardId1, 1), new thrift.ChildInfo(childShardId2, 1)).toJavaList
    }

    "mark_shard_busy" in {
      expect {
        one(nameServer).markShardBusy(shardId, Busy.Busy)
      }
      manager.mark_shard_busy(shardId, Busy.Busy.id)
    }

/*    "copy_shard" in {
      val copyJob = mock[CopyMachine[shards.Shard]]
      val scheduler = mock[JobScheduler]

      expect {
        one(copyManager).newCopyJob(10, 20) willReturn copyJob
        one(copyManager).scheduler willReturn scheduler
        one(copyJob).start(nameServer, scheduler)
      }

      manager.copy_shard(10, 20)
    } */

    "setup_migration" in {
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
    }

/*    "migrate shard" in {
      val migration = new ShardMigration(1, 2, 3, 4)
      val migrateJob = mock[CopyMachine[shards.Shard]]
      val scheduler = mock[JobScheduler]

      expect {
        one(copyManager).newMigrateJob(migration.fromThrift) willReturn migrateJob
        one(copyManager).scheduler willReturn scheduler
        one(migrateJob).start(nameServer, scheduler)
      }

      manager.migrate_shard(migration)
    } */

    "finish_migration" in {
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
    }

    "set_forwarding" in {
      expect {
        one(nameServer).setForwarding(forwarding)
      }
      manager.set_forwarding(thriftForwarding)
    }

    "replace_forwarding" in {
      expect {
        one(nameServer).replaceForwarding(1, 2)
      }
      manager.replace_forwarding(1, 2)
    }

    "get_forwarding" in {
      expect {
        one(nameServer).getForwarding(tableId, 0) willReturn shardInfo1
      }
      manager.get_forwarding(tableId, 0) mustEqual thriftShardInfo1
    }

    "get_forwarding_for_shard" in {
      expect {
        one(nameServer).getForwardingForShard(shardId) willReturn forwarding
      }
      manager.get_forwarding_for_shard(shardId) mustEqual thriftForwarding
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
        one(nameServer).findCurrentForwarding(tableId, 23L) willReturn shardInfo1
      }
      manager.find_current_forwarding(tableId, 23L) mustEqual thriftShardInfo1
    }

    "shard_ids_for_hostname" in {
      expect {
        one(nameServer).shardIdsForHostname(hostname, classname) willReturn List(3, 5)
      }
      manager.shard_ids_for_hostname(hostname, classname) mustEqual List(3, 5).toJavaList
    }

    "shards_for_hostname" in {
      expect {
        one(nameServer).shardsForHostname(hostname, classname) willReturn List(shardInfo1)
      }
      manager.shards_for_hostname(hostname, classname) mustEqual List(thriftShardInfo1).toJavaList
    }

    "get_busy_shards" in {
      expect {
        one(nameServer).getBusyShards() willReturn List(shardInfo1)
      }
      manager.get_busy_shards() mustEqual List(thriftShardInfo1).toJavaList
    }

    "get_parent_shard" in {
      expect {
        one(nameServer).getParentShard(shardId) willReturn shardInfo1
      }
      manager.get_parent_shard(shardId) mustEqual thriftShardInfo1
    }

    "get_root_shard" in {
      expect {
        one(nameServer).getRootShard(shardId) willReturn shardInfo1
      }
      manager.get_root_shard(shardId) mustEqual thriftShardInfo1
    }

    "get_child_shards_of_class" in {
      expect {
        one(nameServer).getChildShardsOfClass(parentShardId, classname) willReturn List(shardInfo1)
      }
      manager.get_child_shards_of_class(parentShardId, classname) mustEqual List(thriftShardInfo1).toJavaList
    }
  }
}
