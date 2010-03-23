package com.twitter.gizzard.nameserver

import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, Busy, ChildInfo}
import org.specs.Specification
import org.specs.mock.JMocker

object SqlNameServerSpec extends Specification with JMocker with Database {
  "SqlNameServer" should {
    val SQL_SHARD = "com.example.SqlShard"

    var nameServer: SqlNameServer = null

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")
    def shardMaterializer(shardInfo: ShardInfo) {
      ()
    }

    doBefore {
      nameServer = new SqlNameServer(queryEvaluator, shardMaterializer)
      nameServer.rebuildSchema()
    }

    "create" in {
      "a new shard" >> {
        expect {
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          expect {
          }

          val shardId = nameServer.createShard(forwardShardInfo)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
          nameServer.createShard(forwardShardInfo)
          nameServer.findShard(forwardShardInfo) mustEqual shardId
        }

        "when the shard contradicts existing data" >> {
          expect {
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
        }

        val shardId = nameServer.createShard(forwardShardInfo)
        nameServer.findShard(forwardShardInfo) mustEqual shardId
        nameServer.getShard(shardId).tablePrefix mustEqual forwardShardInfo.tablePrefix
      }

      "when the shard doesn't exist" >> {
        nameServer.findShard(backwardShardInfo) must throwA[NonExistentShard]
      }
    }

    // XXX: GET SHRARD

    "update" in {
      "existing shard" >> {
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
      }

      val shardId = nameServer.createShard(forwardShardInfo)
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
      val shardId = nameServer.createShard(forwardShardInfo)
      nameServer.markShardBusy(shardId, Busy.Busy)
      nameServer.getShard(shardId).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var shardId: Int = 0
      var forwarding: Forwarding = null

      doBefore {
        shardId = nameServer.createShard(forwardShardInfo)
        forwarding = new Forwarding(0, 1, 0L, shardId)
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
        nameServer.getForwarding(0, 1, 0L).shardId mustEqual shardId
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
      var id1 = 0
      var id2 = 0
      var id3 = 0

      doBefore {
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

