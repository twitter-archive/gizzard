package com.twitter.gizzard.nameserver

import com.twitter.xrayspecs.TimeConversions._
import com.twitter.gizzard.shards.{ShardInfo, ShardId, Busy, LinkInfo, InvalidShard, NonExistentShard}
import com.twitter.gizzard.test.NameServerDatabase
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


class SqlShardSpec extends ConfiguredSpecification with JMocker with ClassMocker with NameServerDatabase {
  lazy val poolConfig = config.configMap("db.connection_pool")

  "SqlShard" should {
    materialize(config.configMap("db"))
    val queryEvaluator = evaluator(config.configMap("db"))

    val SQL_SHARD = "com.example.SqlShard"

    var nameServer: SqlShard = null
    var shardRepository: ShardRepository[Shard] = null

    val forwardShardInfo = new ShardInfo(SQL_SHARD, "forward_table", "localhost")
    val backwardShardInfo = new ShardInfo(SQL_SHARD, "backward_table", "localhost")

    doBefore {
      nameServer = new SqlShard(queryEvaluator)
      nameServer.rebuildSchema()
      reset(config.configMap("db"))
      shardRepository = mock[ShardRepository[Shard]]
    }

    "create" in {
      "a new shard" >> {
        nameServer.createShard(forwardShardInfo)
        nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
      }

      "when the shard already exists" >> {
        "when the shard matches existing data" >> {
          nameServer.createShard(forwardShardInfo)
          nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
          nameServer.createShard(forwardShardInfo)
          nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
        }

        "when the shard contradicts existing data" >> {
          nameServer.createShard(forwardShardInfo)
          val otherShard = forwardShardInfo.clone()
          otherShard.className = "garbage"
          nameServer.createShard(otherShard) must throwA[InvalidShard]
        }
      }
    }

    "find" in {
      "a created shard" >> {
        nameServer.createShard(forwardShardInfo)
        nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
        nameServer.getShard(forwardShardInfo.id).className mustEqual forwardShardInfo.className
      }

      "when the shard doesn't exist" >> {
        nameServer.getShard(backwardShardInfo.id) must throwA[NonExistentShard]
      }
    }

    // FIXME: GET SHARD

    "delete" in {
      nameServer.createShard(forwardShardInfo)
      nameServer.getShard(forwardShardInfo.id) mustEqual forwardShardInfo
      nameServer.deleteShard(forwardShardInfo.id)
      nameServer.getShard(forwardShardInfo.id) must throwA[NonExistentShard]
    }

    "children" in {
      def shard(i: Int) = ShardId("host", i.toString)
      def linkInfo(up: Int, down: Int, weight: Int) = LinkInfo(shard(up), shard(down), weight)
      def link = linkInfo(1, _: Int, _: Int)

      "add & find" >> {
        nameServer.addLink(shard(1), shard(100), 2)
        nameServer.addLink(shard(1), shard(200), 2)
        nameServer.addLink(shard(1), shard(300), 1)
        nameServer.listDownwardLinks(shard(1)) mustEqual
          List(link(100, 2), link(200, 2), link(300, 1))
      }

      "remove" >> {
        nameServer.addLink(shard(1), shard(100), 2)
        nameServer.addLink(shard(1), shard(200), 2)
        nameServer.addLink(shard(1), shard(300), 1)
        nameServer.removeLink(shard(1), shard(200))
        nameServer.listDownwardLinks(shard(1)) mustEqual List(link(100, 2), link(300, 1))
      }

      "add & remove, retaining order" >> {
        nameServer.addLink(shard(1), shard(100), 5)
        nameServer.addLink(shard(1), shard(200), 2)
        nameServer.addLink(shard(1), shard(300), 1)
        nameServer.removeLink(shard(1), shard(200))
        nameServer.addLink(shard(1), shard(150), 8)
        nameServer.listDownwardLinks(shard(1)) mustEqual List(link(150, 8), link(100, 5), link(300, 1))
      }
    }

    "set shard busy" in {
      nameServer.createShard(forwardShardInfo)
      nameServer.markShardBusy(forwardShardInfo.id, Busy.Busy)
      nameServer.getShard(forwardShardInfo.id).busy mustEqual Busy.Busy
    }

    "forwarding changes" in {
      var forwarding: Forwarding = null

      doBefore {
        nameServer.createShard(forwardShardInfo)
        forwarding = new Forwarding(1, 0L, forwardShardInfo.id)
      }

      "set and get for shard" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwardingsForShard(forwarding.shardId) mustEqual List(forwarding)
      }

      "replace" in {
        val newShardId = ShardId("new", "shard")
        nameServer.setForwarding(forwarding)
        nameServer.replaceForwarding(forwardShardInfo.id, newShardId)
        nameServer.getForwardingsForShard(newShardId).map(_.shardId) mustEqual List(newShardId)
      }

      "set and get" in {
        nameServer.setForwarding(forwarding)
        nameServer.getForwarding(1, 0L).shardId mustEqual forwardShardInfo.id
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

      doBefore {
        nameServer.createShard(shard1)
        nameServer.createShard(shard2)
        nameServer.createShard(shard3)
        nameServer.addLink(shard1.id, shard2.id, 10)
        nameServer.addLink(shard2.id, shard3.id, 10)
      }

      "shardsForHostname" in {
        nameServer.shardsForHostname("localhost").map { _.id }.sort(_.tablePrefix < _.tablePrefix) mustEqual List(shard1.id, shard2.id, shard3.id).sort(_.tablePrefix < _.tablePrefix)
      }

      "getBusyShards" in {
        nameServer.getBusyShards() mustEqual List()
        nameServer.markShardBusy(shard1.id, Busy.Busy)
        nameServer.getBusyShards().map { _.id } mustEqual List(shard1.id)
      }

      "getParentShard" in {
        nameServer.listUpwardLinks(shard3.id) mustEqual List(LinkInfo(shard2.id, shard3.id, 10))
        nameServer.listUpwardLinks(shard2.id) mustEqual List(LinkInfo(shard1.id, shard2.id, 10))
        nameServer.listUpwardLinks(shard1.id) mustEqual List()
      }

      "getChildShardsOfClass" in {
        nameServer.getChildShardsOfClass(shard1.id, SQL_SHARD).map { _.id } mustEqual List(shard2.id, shard3.id)
      }
    }
  }
}
