package com.twitter.gizzard.nameserver

import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, Busy, ChildInfo}
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class MemoryShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "MemoryShard" should {
    val SQL_SHARD = "com.example.SqlShard"

    var nameServer: MemoryShard = null
    var shardRepository: ShardRepository[Shard] = null

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")

    doBefore {
      nameServer = new MemoryShard()
      shardRepository = mock[ShardRepository[Shard]]
    }

    "create" in {
      "a new shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          expect {
            exactly(2).of(shardRepository).create(forwardShardInfo)
          }

          val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
          nameServer.createShard(forwardShardInfo, shardRepository)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
        }

        "when the shard contradicts existing data" >> {
          expect {
            one(shardRepository).create(forwardShardInfo)
          }

          nameServer.createShard(forwardShardInfo, shardRepository)
          val otherShard = forwardShardInfo.clone()
          otherShard.className = "garbage"
          nameServer.createShard(otherShard, shardRepository) must throwA[InvalidShard]
        }
      }
    }

    "find" in {
      "a created shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
        nameServer.getShard(shardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      }

      "when the shard doesn't exist" >> {
        nameServer.findShard(backwardShardInfo) must throwA[NonExistentShard]
      }
    }

    // FIXME: GET SHARD

    "update" in {
      "existing shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
        val otherShardInfo = forwardShardInfo.clone
        otherShardInfo.tablePrefix = "other_table"
        otherShardInfo.shardId = shardId
        nameServer.updateShard(otherShardInfo)
        nameServer.getShard(shardId) mustEqual otherShardInfo
      }

      "nonexistent shard" >> {
        nameServer.updateShard(new ShardInfo(SQL_SHARD, "other_table", "localhost", "", "", Busy.Normal, 500)) must throwA[NonExistentShard]
      }
    }

    "delete" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
      nameServer.findShard(forwardShardInfo) mustEqual shardId
      nameServer.deleteShard(shardId)
      nameServer.findShard(forwardShardInfo) must throwA[NonExistentShard]
    }

    "children" in {
      "add & find" >> {
        nameServer.addChildShard(1, 100, 2)
        nameServer.addChildShard(1, 200, 2)
        nameServer.addChildShard(1, 300, 1)
        nameServer.listShardChildren(1) mustEqual
          List(ChildInfo(100, 2), ChildInfo(200, 2), ChildInfo(300, 1))
      }

      "remove" >> {
        nameServer.addChildShard(1, 100, 2)
        nameServer.addChildShard(1, 200, 2)
        nameServer.addChildShard(1, 300, 1)
        nameServer.removeChildShard(1, 200)
        nameServer.listShardChildren(1) mustEqual List(ChildInfo(100, 2), ChildInfo(300, 1))
      }

      "add & remove, retaining order" >> {
        nameServer.addChildShard(1, 100, 5)
        nameServer.addChildShard(1, 200, 2)
        nameServer.addChildShard(1, 300, 1)
        nameServer.removeChildShard(1, 200)
        nameServer.addChildShard(1, 150, 8)
        nameServer.listShardChildren(1) mustEqual List(ChildInfo(150, 8), ChildInfo(100, 5), ChildInfo(300, 1))
      }

      "replace" >> {
        nameServer.addChildShard(1, 100, 2)
        nameServer.addChildShard(1, 200, 2)
        nameServer.addChildShard(1, 300, 1)
        nameServer.replaceChildShard(300, 400)
        nameServer.listShardChildren(1) mustEqual List(ChildInfo(100, 2), ChildInfo(200, 2), ChildInfo(400, 1))
      }
    }

    "set shard busy" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      val shardId = nameServer.createShard(forwardShardInfo, shardRepository)
      nameServer.markShardBusy(shardId, Busy.Busy)
      nameServer.getShard(shardId).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var shardId: Int = 0
      var forwarding: Forwarding = null

      doBefore {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        shardId = nameServer.createShard(forwardShardInfo, shardRepository)
        forwarding = new Forwarding(1, 0L, shardId)
      }

      "set and get for shard" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardingForShard(forwarding.shardId) mustEqual forwarding
      }

      "replace" in {
        val newShardId = 2
        nameServer.setForwarding(forwarding)
        nameServer.replaceForwarding(forwarding.shardId, newShardId)
        nameServer.getForwardingForShard(2).shardId mustEqual newShardId
      }

      "set and get" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwarding(1, 0L).shardId mustEqual shardId
      }

      "get all" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardings() mustEqual List(forwarding)
      }
    }

    "advanced shard navigation" in {
      val shard1 = new ShardInfo(SQL_SHARD, "forward_1", "localhost")
      val shard2 = new ShardInfo(SQL_SHARD, "forward_1_also", "localhost")
      val shard3 = new ShardInfo(SQL_SHARD, "forward_1_too", "localhost")
      shard1.shardId = 101
      shard2.shardId = 102
      shard3.shardId = 103

      doBefore {
        expect {
          one(shardRepository).create(shard1)
          one(shardRepository).create(shard2)
          one(shardRepository).create(shard3)
        }

        nameServer.createShard(shard1, shardRepository)
        nameServer.createShard(shard2, shardRepository)
        nameServer.createShard(shard3, shardRepository)
        nameServer.addChildShard(shard1.shardId, shard2.shardId, 10)
        nameServer.addChildShard(shard2.shardId, shard3.shardId, 10)
      }

      "shardIdsForHostname" in {
        nameServer.shardIdsForHostname("localhost", SQL_SHARD).sort(_ < _) mustEqual List(shard1.shardId, shard2.shardId, shard3.shardId).sort(_ < _)
      }

      "shardsForHostname" in {
        nameServer.shardsForHostname("localhost", SQL_SHARD).map { _.shardId }.sort(_ < _) mustEqual List(shard1.shardId, shard2.shardId, shard3.shardId).sort(_ < _)
      }

      "getBusyShards" in {
        nameServer.getBusyShards() mustEqual List()
        nameServer.markShardBusy(shard1.shardId, Busy.Busy)
        nameServer.getBusyShards().map { _.shardId } mustEqual List(shard1.shardId)
      }

      "getParentShard" in {
        nameServer.getParentShard(shard3.shardId).shardId mustEqual shard2.shardId
        nameServer.getParentShard(shard2.shardId).shardId mustEqual shard1.shardId
        nameServer.getParentShard(shard1.shardId).shardId mustEqual shard1.shardId
      }

      "getRootShard" in {
        nameServer.getRootShard(shard3.shardId).shardId mustEqual shard1.shardId
      }

      "getChildShardsOfClass" in {
        nameServer.getChildShardsOfClass(shard1.shardId, SQL_SHARD).map { _.shardId } mustEqual List(shard2.shardId, shard3.shardId)
      }
    }
  }
}
