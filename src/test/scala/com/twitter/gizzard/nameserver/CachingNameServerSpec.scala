package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards.{ShardInfo, Busy, ChildInfo}
import org.specs.Specification
import org.specs.mock.JMocker

object CachingNameServerSpec extends Specification with JMocker {
  "CachingNameServer" should {
    val SQL_SHARD = "com.example.SqlShard"

    val managingNameServer = mock[ManagingNameServer]
    val mappingFunction = (n: Long) => n
    var nameServer: CachingNameServer = null

    val shards = (1 until 5).force.map { id => new ShardInfo(SQL_SHARD, "test", "localhost", "a", "b", Busy.Normal, id) }.toList
    val childrenList = List(new ChildInfo(4, 1))
    val shardChildren = Map(3 -> childrenList)
    val shardForwardings = List(new Forwarding(1, 1, 1), new Forwarding(1, 2, 2), new Forwarding(1, 3, 3), new Forwarding(2, 1, 4))
    doBefore {
      expect {
        one(managingNameServer).listShards() willReturn shards
        one(managingNameServer).listShardChildren() willReturn shardChildren
        one(managingNameServer).getForwardings() willReturn shardForwardings
      }

      nameServer = new CachingNameServer(managingNameServer, mappingFunction)
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
      nameServer.findCurrentForwarding(1, 2) mustEqual shards(1)
    }
  }
}
