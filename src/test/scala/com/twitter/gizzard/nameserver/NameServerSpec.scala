package com.twitter.gizzard.nameserver

import java.sql.SQLException
import scala.collection.mutable
import com.twitter.gizzard.shards.{ShardInfo, Shard, Busy, ChildInfo}
import com.twitter.xrayspecs.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import jobs.{JobScheduler}
import shards.ShardException


object NameServerSpec extends Specification with JMocker with ClassMocker with Database {
  "NameServer" should {
    val SQL_SHARD = "com.example.SqlShard"
    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")
    val otherShardInfo = new ShardInfo(SQL_SHARD, "other_table", "localhost")

    var shardRepository: ShardRepository[Shard] = null
//    var copyManager: CopyManager[Shard] = null
    var nameServer: ManagingNameServer = null
    var forwardShard: Shard = null
    var backwardShard: Shard = null

    doBefore {
      try {
        queryEvaluator.execute("DELETE FROM shards")
      } catch {
        case e: SQLException =>
      }
      try {
        queryEvaluator.execute("DELETE FROM shard_children")
      } catch {
        case e: SQLException =>
      }

      shardRepository = mock[ShardRepository[Shard]]
      nameServer = new SqlNameServer(queryEvaluator, (a: ShardInfo) => ())

//      nameServer.reload()

      forwardShard = mock[Shard]
      backwardShard = mock[Shard]
    }

    "create" in {
      "a new shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          expect {
            exactly(2).of(shardRepository).create(forwardShardInfo)
          }

          val shardId = nameServer.createShard(forwardShardInfo)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
          nameServer.createShard(forwardShardInfo)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
        }

        "when the shard contradicts existing data" >> {
          expect {
            one(shardRepository).create(forwardShardInfo)
          }

          nameServer.createShard(forwardShardInfo)
          val otherShard = forwardShardInfo.clone()
          otherShard.className = "garbage"
          nameServer.createShard(otherShard) must throwA[InvalidShard]
        }
      }
    }

    "find" in {
      "a created shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
        nameServer.getShard(shardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      }

      "when the shard doesn't exist" >> {
        nameServer.findShard(backwardShardInfo) must throwA[NonExistentShard]
      }
    }

    "update" in {
      "existing shard" >> {
        expect {
          one(shardRepository).create(forwardShardInfo)
        }

        val shardId = nameServer.createShard(forwardShardInfo)
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

      val shardId = nameServer.createShard(forwardShardInfo)
      nameServer.findShard(forwardShardInfo) mustEqual shardId
      nameServer.deleteShard(shardId)
      nameServer.findShard(forwardShardInfo) must throwA[NonExistentShard]
    }

    "add & find children" in {
      nameServer.addChildShard(1, 100, 2)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.listShardChildren(1) mustEqual
        List(ChildInfo(100, 2), ChildInfo(200, 2), ChildInfo(300, 1))
    }

    "remove children" in {
      nameServer.addChildShard(1, 100, 2)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.removeChildShard(1, 200)
      nameServer.listShardChildren(1) mustEqual List(ChildInfo(100, 2), ChildInfo(300, 1))
    }

    "add & remove children, retaining order" in {
      nameServer.addChildShard(1, 100, 5)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.removeChildShard(1, 200)
      nameServer.addChildShard(1, 150, 8)
      nameServer.listShardChildren(1) mustEqual List(ChildInfo(150, 8), ChildInfo(100, 5), ChildInfo(300, 1))
    }

    "replace children" in {
      nameServer.addChildShard(1, 100, 2)
      nameServer.addChildShard(1, 200, 2)
      nameServer.addChildShard(1, 300, 1)
      nameServer.replaceChildShard(300, 400)
      nameServer.listShardChildren(1) mustEqual List(ChildInfo(100, 2), ChildInfo(200, 2), ChildInfo(400, 1))
    }

    "set shard busy" in {
      expect {
        one(shardRepository).create(forwardShardInfo)
      }

      val shardId = nameServer.createShard(forwardShardInfo)
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

        shardId = nameServer.createShard(forwardShardInfo)
//        forwarding = new Forwarding(0, List(1, 2), 0L, shardId)
        forwarding = new Forwarding(0, 1, 0L, shardId)
      }

      "setForwarding" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardingForShard(forwarding.shardId) mustEqual forwarding
      }

      "replaceForwarding" in {
        val newShardId = 2
        nameServer.setForwarding(forwarding)
        nameServer.replaceForwarding(forwarding.shardId, newShardId)
        nameServer.getForwardingForShard(2).shardId mustEqual newShardId
      }

      "getForwarding" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwarding(4, 0L).shardId mustEqual shardId
      }

      "getForwardingForShard" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardingForShard(shardId) mustEqual forwarding
      }

      "getForwardings" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardings() mustEqual List(forwarding)
      }

/*      "findCurrentForwarding" in {
        forwardShardInfo.shardId = shardId
        expect {
          one(shardRepository).find(forwardShardInfo, 1, List()) willReturn forwardShard
        }
        nameServer.setForwarding(forwarding)
        nameServer.findCurrentForwarding(List(1, 2), 0L) mustEqual forwardShard
      } */
    }

    "advanced shard navigation" in {
      val shard1 = new ShardInfo(SQL_SHARD, "forward_1", "localhost")
      val shard2 = new ShardInfo(SQL_SHARD, "forward_1_also", "localhost")
      val shard3 = new ShardInfo(SQL_SHARD, "forward_1_too", "localhost")
      var id1 = 0
      var id2 = 0
      var id3 = 0

      doBefore {
        expect {
          one(shardRepository).create(shard1)
          one(shardRepository).create(shard2)
          one(shardRepository).create(shard3)
        }
        id1 = nameServer.createShard(shard1)
        id2 = nameServer.createShard(shard2)
        id3 = nameServer.createShard(shard3)
        nameServer.addChildShard(id1, id2, 10)
        nameServer.addChildShard(id2, id3, 10)
      }

      "shardIdsForHostname" in {
        nameServer.shardIdsForHostname("localhost", SQL_SHARD) mustEqual List(id1, id2, id3)
      }

      "shardsForHostname" in {
        nameServer.shardsForHostname("localhost", SQL_SHARD).map { _.shardId } mustEqual List(id1, id2, id3)
      }

      "getBusyShards" in {
        nameServer.getBusyShards() mustEqual List()
        nameServer.markShardBusy(id1, Busy.Busy)
        nameServer.getBusyShards().map { _.shardId } mustEqual List(id1)
      }

      "getParentShard" in {
        nameServer.getParentShard(id3).shardId mustEqual id2
        nameServer.getParentShard(id2).shardId mustEqual id1
        nameServer.getParentShard(id1).shardId mustEqual id1
      }

      "getRootShard" in {
        nameServer.getRootShard(id3).shardId mustEqual id1
      }

      "getChildShardsOfClass" in {
        nameServer.getChildShardsOfClass(id1, SQL_SHARD).map { _.shardId } mustEqual List(id2, id3)
      }
    }
  }
}
