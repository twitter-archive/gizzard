package com.twitter.gizzard.sharding

import org.specs.mock.{ClassMocker, JMocker}
import org.specs.Specification
import com.twitter.gizzard.Conversions._


object ShardManagerSpec extends Specification with JMocker with ClassMocker {
  var manager: ShardManager[Int, Shard] = null
  var nameServer: NameServer[Int, Shard] = null
  val thriftForwardShardInfo = new thrift.ShardInfo("com.twitter.service.flock.edges.SqlShard",
    "forward_table_prefix", "forward_hostname", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal.id, 1)
  val forwardShardInfo = new ShardInfo("com.twitter.service.flock.edges.SqlShard",
    "forward_table_prefix", "forward_hostname", "INT UNSIGNED", "INT UNSIGNED", Busy.Normal, 1)
  val shardId = 100
  val childShardId1 = 200
  val childShardId2 = 201

  "ShardManager" should {
    doBefore {
      nameServer = mock[NameServer[Int, Shard]]
      manager = new ShardManager(nameServer)
    }

    "create_shard" in {
      expect {
        one(nameServer).createShard(forwardShardInfo) willReturn shardId
      }
      manager.create_shard(thriftForwardShardInfo) mustEqual shardId
    }

    "find_shard" in {
      expect {
        one(nameServer).findShard(forwardShardInfo) willReturn shardId
      }
      manager.find_shard(thriftForwardShardInfo) mustEqual shardId
    }

    "get_shard" in {
      expect {
        one(nameServer).getShard(shardId) willReturn forwardShardInfo
      }
      manager.get_shard(shardId) mustEqual thriftForwardShardInfo
    }

    "update_shard" in {
      expect {
        one(nameServer).updateShard(forwardShardInfo)
      }
      manager.update_shard(thriftForwardShardInfo)
    }

    "delete_shard" in {
      expect {
        one(nameServer).deleteShard(shardId)
      }
      manager.delete_shard(shardId)
    }

    "add_child_shard" in {
      expect {
        one(nameServer).addChildShard(shardId, childShardId1, 1, 2)
      }
      manager.add_child_shard(shardId, childShardId1, 1, 2)
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
        one(nameServer).listShardChildren(shardId) willReturn List(new ChildInfo(childShardId1, 1, 1), new ChildInfo(childShardId2, 2, 1))
      }
      manager.list_shard_children(shardId) mustEqual
        List(new thrift.ChildInfo(childShardId1, 1, 1), new thrift.ChildInfo(childShardId2, 2, 1)).toJavaList
    }

    "mark_shard_busy" in {
      expect {
        one(nameServer).markShardBusy(shardId, Busy.Busy)
      }
      manager.mark_shard_busy(shardId, Busy.Busy.id)
    }

    "set_forwarding" in {
      val forwarding = new Forwarding(List(1, 2, 3), 0, 12)
      val thriftForwarding = new thrift.Forwarding(List(1, 2, 3).toJavaList, 0, 12)
      expect {
        one(nameServer).setForwarding(forwarding)
      }
      manager.set_forwarding(thriftForwarding)
    }

    "get_forwarding" in {
      val tableId = List(1, 2, 3)
      expect {
        one(nameServer).getForwarding(tableId, 0) willReturn forwardShardInfo
      }
      manager.get_forwarding(tableId.toJavaList, 0) mustEqual thriftForwardShardInfo
    }

    "get_forwarding_for_shard" in {
      val shardId = 23
      val forwarding = new Forwarding(List(1, 2, 3), 0, shardId)
      val thriftForwarding = new thrift.Forwarding(List(1, 2, 3).toJavaList, 0, shardId)
      expect {
        one(nameServer).getForwardingForShard(shardId) willReturn forwarding
      }
      manager.get_forwarding_for_shard(shardId) mustEqual thriftForwarding
    }

    "get_forwardings" in {
      val shardId = 23
      val forwarding = new Forwarding(List(1, 2, 3), 0, shardId)
      val thriftForwarding = new thrift.Forwarding(List(1, 2, 3).toJavaList, 0, shardId)
      expect {
        one(nameServer).getForwardings() willReturn List(forwarding)
      }
      manager.get_forwardings() mustEqual List(thriftForwarding).toJavaList
    }

    "replace_forwarding" in {
      expect {
        one(nameServer).replaceForwarding(1, 2)
      }
      manager.replace_forwarding(1, 2)
    }

    "reload_forwardings" in {
      expect {
        one(nameServer).reload()
      }
      manager.reload_forwardings()
    }

    "shard_ids_for_hostname" in {
      val hostname = "host1"
      val classname = "com.example.Classname"
      expect {
        one(nameServer).shardIdsForHostname(hostname, classname) willReturn List(3, 5)
      }
      manager.shard_ids_for_hostname(hostname, classname) mustEqual List(3, 5).toJavaList
    }

    "shards_for_hostname" in {
      val hostname = "host1"
      val classname = "com.example.Classname"
      expect {
        one(nameServer).shardsForHostname(hostname, classname) willReturn List(forwardShardInfo)
      }
      manager.shards_for_hostname(hostname, classname) mustEqual List(thriftForwardShardInfo).toJavaList
    }

    "get_busy_shards" in {
      expect {
        one(nameServer).getBusyShards() willReturn List(forwardShardInfo)
      }
      manager.get_busy_shards() mustEqual List(thriftForwardShardInfo).toJavaList
    }

    "get_parent_shard" in {
      val shardId = 23
      expect {
        one(nameServer).getParentShard(shardId) willReturn forwardShardInfo
      }
      manager.get_parent_shard(shardId) mustEqual thriftForwardShardInfo
    }

    "get_root_shard" in {
      val shardId = 23
      expect {
        one(nameServer).getRootShard(shardId) willReturn forwardShardInfo
      }
      manager.get_root_shard(shardId) mustEqual thriftForwardShardInfo
    }

    "get_child_shards_of_class" in {
      val parentShardId = 900
      val classname = "com.example.Classname"
      expect {
        one(nameServer).getChildShardsOfClass(parentShardId, classname) willReturn List(forwardShardInfo)
      }
      manager.get_child_shards_of_class(parentShardId, classname) mustEqual List(thriftForwardShardInfo).toJavaList
    }
  }
}
