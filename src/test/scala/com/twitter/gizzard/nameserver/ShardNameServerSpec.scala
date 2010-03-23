package com.twitter.gizzard.nameserver

import com.twitter.gizzard.shards.{ShardInfo, Busy, ChildInfo}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import shards._


object ShardNameServerSpec extends Specification with JMocker with ClassMocker {
  "ShardNameServer" should {
    var nameServer = mock[CachingNameServer]
    var shardRepository = mock[ShardRepository[shards.Shard]]
    var shardNameServer: ShardNameServer[shards.Shard] = null

    var shardInfo = new ShardInfo("test", "test", "test")
    var children = List(new ChildInfo(2, 2))
    var blankChildren: Seq[ChildInfo] = List()

    var shard = mock[Shard]

    doBefore {
      shardNameServer = new ShardNameServer[shards.Shard](nameServer, shardRepository)
    }

    "find shard by id" in  {
      expect {
        one(nameServer).getShardInfo(1) willReturn shardInfo
        one(nameServer).getChildren(1) willReturn children
        one(nameServer).getShardInfo(2) willReturn shardInfo
        one(nameServer).getChildren(2) willReturn blankChildren
        one(shardRepository).find(shardInfo, 2, List()) willReturn shard
        one(shardRepository).find(shardInfo, 1, List(shard)) willReturn shard
      }

      shardNameServer.findShardById(1) mustEqual shard
    }
  }
}

