package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards.{ShardInfo, Busy, ChildInfo}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object NameServerSpec extends Specification with JMocker with ClassMocker {
  "NameServer" should {
    val SQL_SHARD = "com.example.SqlShard"

    val nameServerShard = mock[Shard]
    var shardRepository = mock[ShardRepository[gizzard.shards.Shard]]
    val mappingFunction = (n: Long) => n
    var nameServer: NameServer[gizzard.shards.Shard] = null

    val shards = (1 until 5).force.map { id => new ShardInfo(SQL_SHARD, "test", "localhost", "a", "b", Busy.Normal, id) }.toList
    val childrenList = List(new ChildInfo(4, 1))
    val shardChildren = Map(3 -> childrenList)
    val shardForwardings = List(new Forwarding(1, 1, 1), new Forwarding(1, 2, 2), new Forwarding(1, 3, 3), new Forwarding(2, 1, 4))
    var shard = mock[gizzard.shards.Shard]

    doBefore {
      expect {
        one(nameServerShard).reload()
        one(nameServerShard).listShards() willReturn shards
        one(nameServerShard).listShardChildren() willReturn shardChildren
        one(nameServerShard).getForwardings() willReturn shardForwardings
      }

      nameServer = new NameServer[gizzard.shards.Shard](nameServerShard, shardRepository, mappingFunction)
      nameServer.reload()
    }

    "reload and get shard info" in {
      nameServer.getShardInfo(1) mustEqual shards(0)
      nameServer.getShardInfo(2) mustEqual shards(1)
      nameServer.getShardInfo(3) mustEqual shards(2)
      nameServer.getShardInfo(4) mustEqual shards(3)
    }

    "get children" in {
      nameServer.getChildren(3) mustEqual childrenList
    }

    "find current forwarding" in {
      expect {
        one(shardRepository).find(shards(1), 1, List()) willReturn shard
      }

      nameServer.findCurrentForwarding(1, 2) mustEqual shard
    }

    "find shard by id" in {
      expect {
        one(shardRepository).find(shards(3), 1, List()) willReturn shard
        one(shardRepository).find(shards(2), 1, List(shard)) willReturn shard
      }

      nameServer.findShardById(3) mustEqual shard
    }

    "create shard" in {
      expect {
        one(nameServerShard).createShard(shards(0), shardRepository) willThrow new InvalidShard
        one(nameServerShard).createShard(shards(0), shardRepository) willReturn 23
      }
      nameServer.createShard(shards(0)) mustEqual 23
    }
  }
}
