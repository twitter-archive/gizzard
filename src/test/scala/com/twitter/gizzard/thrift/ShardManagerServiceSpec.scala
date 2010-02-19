package com.twitter.gizzard.thrift

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.thrift.conversions.Sequences._
import shards.Busy


object ShardManagerServiceSpec extends Specification with JMocker with ClassMocker {
  val nameServer = mock[nameserver.NameServer[shards.Shard]]
  val manager = new thrift.ShardManagerService(nameServer)
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
  val tableId = List(1, 2, 3)
  val forwarding = new nameserver.Forwarding(List(1, 2, 3), 0, shardId)
  val thriftForwarding = new thrift.Forwarding(List(1, 2, 3).toJavaList, 0, shardId)
  val shard = new shards.Shard {
    def shardInfo = shardInfo1
    def weight = 3
  }


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

    "copy_shard" in {
      expect {
        one(nameServer).copyShard(1, 2)
      }
      manager.copy_shard(1, 2)
    }

    "setup_migration" in {
      expect {
        one(nameServer).setupMigration(shardInfo1, shardInfo2) willReturn new nameserver.ShardMigration(1, 2, 3, 4)
      }
      manager.setup_migration(thriftShardInfo1, thriftShardInfo2) mustEqual new thrift.ShardMigration(1, 2, 3, 4)
    }

    "migrate_shard" in {
      expect {
        one(nameServer).migrateShard(new nameserver.ShardMigration(1, 2, 3, 4))
      }
      manager.migrate_shard(new thrift.ShardMigration(1, 2, 3, 4))
    }

    "finish_migration" in {
      expect {
        one(nameServer).finishMigration(new nameserver.ShardMigration(1, 2, 3, 4))
      }
      manager.finish_migration(new thrift.ShardMigration(1, 2, 3, 4))
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
      manager.get_forwarding(tableId.toJavaList, 0) mustEqual thriftShardInfo1
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
        one(nameServer).findCurrentForwarding(tableId, 23L) willReturn shard
      }
      manager.find_current_forwarding(tableId.toJavaList, 23L) mustEqual thriftShardInfo1
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
