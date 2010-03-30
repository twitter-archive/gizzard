package com.twitter.gizzard.shards

import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.{ThrottledLogger, Logger}
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.ostrich.W3CReporter


object ReplicatingShardSpec extends Specification with JMocker {
  "ReplicatingShard" should {
    val shard1 = mock[fake.Shard]
    val shard2 = mock[fake.Shard]
    val shard3 = mock[fake.Shard]
    val future = new Future("Future!", 1, 1, 1.second, 1.second)
    val log = new ThrottledLogger[String](Logger(), 1, 1)
    val shards = List(shard1, shard2)
    val loadBalancer = () => shards
    var replicatingShard = new fake.ReadWriteShardAdapter(new ReplicatingShard(null, 1, shards, loadBalancer, log, future, new W3CReporter(Logger())))

    "failover" in {
      "when shard1 throws an exception" in {
        val shard1Info = new ShardInfo("", "table_prefix", "hostname")
        val exception = new ShardException("o noes")
        expect {
          one(shard1).shardInfo.willReturn(shard1Info)
          one(shard1).getName.willThrow(exception) then
          one(shard2).getName.willReturn("bob")
        }
        replicatingShard.getName mustEqual "bob"
      }

      "when all shards throw an exception" in {
        val shard1Info = new ShardInfo("", "table_prefix", "hostname")
        val exception = new ShardException("o noes")
        expect {
          one(shard1).shardInfo willReturn shard1Info
          one(shard2).shardInfo willReturn shard1Info
          one(shard1).getName.willThrow(exception)
          one(shard2).getName.willThrow(exception)
        }
        replicatingShard.getName must throwA[ShardException]
      }
    }

    "writes happen to all shards" in {
      "when they succeed" in {
        expect {
          one(shard1).setName("alice")
          one(shard2).setName("alice")
        }
        replicatingShard.setName("alice")
      }

      "when the first one fails" in {
        expect {
          one(shard1).setName("alice").willThrow(new ShardException("o noes"))
          one(shard2).setName("alice")
        }
        replicatingShard.setName("alice") must throwA[Exception]
      }
    }

    "reads happen to shards in order" in {
      expect {
        one(shard1).getName.willReturn("ted")
      }
      replicatingShard.getName mustEqual "ted"
    }
  }
}
