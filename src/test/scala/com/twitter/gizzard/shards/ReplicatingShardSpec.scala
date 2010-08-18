package com.twitter.gizzard.shards

import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.Logger
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.ostrich.W3CReporter


object ReplicatingShardSpec extends ConfiguredSpecification with JMocker {
  "ReplicatingShard" should {
    val shard1 = mock[fake.Shard]
    val shard2 = mock[fake.Shard]
    val shard3 = mock[fake.Shard]
    val future = new Future("Future!", 1, 1, 1.second, 1.second)
    val shards = List(shard1, shard2)
    val loadBalancer = () => shards
    var replicatingShard = new fake.ReadWriteShardAdapter(new ReplicatingShard(null, 1, shards, loadBalancer, future))

    "failover" in {
      "when shard1 throws an exception" in {
        val shard1Info = new ShardInfo("", "table_prefix", "hostname")
        val exception = new ShardException("o noes")
        expect {
          one(shard1).shardInfo.willReturn(shard1Info)
          one(shard1).get("name").willThrow(exception) then
          one(shard2).get("name").willReturn(Some("bob"))
        }
        replicatingShard.get("name") mustEqual Some("bob")
      }

      "when all shards throw an exception" in {
        val shard1Info = new ShardInfo("", "table_prefix", "hostname")
        val exception = new ShardException("o noes")
        expect {
          one(shard1).shardInfo willReturn shard1Info
          one(shard2).shardInfo willReturn shard1Info
          one(shard1).get("name").willThrow(exception)
          one(shard2).get("name").willThrow(exception)
        }
        replicatingShard.get("name") must throwA[ShardException]
      }
    }

    "writes happen to all shards" in {
      "when they succeed" in {
        expect {
          one(shard1).put("name", "alice")
          one(shard2).put("name", "alice")
        }
        replicatingShard.put("name", "alice")
      }

      "when the first one fails" in {
        expect {
          one(shard1).put("name", "alice").willThrow(new ShardException("o noes"))
          one(shard2).put("name", "alice")
        }
        replicatingShard.put("name", "alice") must throwA[Exception]
      }
    }

    "reads happen to shards in order" in {
      expect {
        one(shard1).get("name").willReturn(Some("ted"))
      }
      replicatingShard.get("name") mustEqual Some("ted")
    }
  }
}
