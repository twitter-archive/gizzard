package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import shards._

object ShardRouterSpec extends Specification with JMocker with ClassMocker {
  "ShardRouter" should {
    var shardRouter: ShardRouter[shards.Shard, Int] = null
    val nameServer = mock[ShardNameServer[shards.Shard]]
    def translate(a: Int): Int = -1 * a

    val shardInfo = mock[ShardInfo]
    val shard = mock[shards.Shard]

    doBefore {
      shardRouter = new ShardRouter(nameServer, translate)
    }

    "route" in {
      expect {
        one(nameServer).findCurrentForwarding(-1, 1) willReturn shardInfo
        one(shardInfo).shardId willReturn 2
        one(nameServer).findShardById(2) willReturn shard
      }

      shardRouter(1, 1) mustEqual shard
    }
  }
}
