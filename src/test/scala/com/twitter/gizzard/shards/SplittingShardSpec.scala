package com.twitter.gizzard.shards

import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.{ThrottledLogger, Logger}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.nameserver.LoadBalancer
import com.twitter.ostrich.W3CReporter
import nameserver.{ShardRepository, NameServer}

object SplittingShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "SplittingShard" should {
    val nameServer = mock[nameserver.SplittingNameServer[fake.Shard]]
    val shard1 = mock[fake.ConcreteShard]
    val shard2 = mock[fake.ConcreteShard]
    val shards = List(shard1, shard2)
    var splittingShard = new fake.ReadWriteShardAdapter(new SplittingShard(nameServer, null, 1, shards))
    
    "splitting behavior" in {
      "writes should go to the shard specified in the nameserver" in {
        expect {
          val address:(Int, Long) = (1, "a".charAt(0).asDigit)
          one(nameServer).findCurrentForwarding(address).willReturn(shard1)
          one(shard1).put("a", "b")
          never(shard2).put("a", "b")
        }
        
        splittingShard.put("a", "b")
      }
    }
    
    // "failover" in {
    //   "when shard1 throws an exception" in {
    //     val shard1Info = new ShardInfo("", "table_prefix", "hostname")
    //     val exception = new ShardException("o noes")
    //     expect {
    //       one(shard1).shardInfo.willReturn(shard1Info)
    //       one(shard1).getName.willThrow(exception) then
    //       one(shard2).getName.willReturn("bob")
    //     }
    //     replicatingShard.getName mustEqual "bob"
    //   }
    // 
    //   "when all shards throw an exception" in {
    //     val shard1Info = new ShardInfo("", "table_prefix", "hostname")
    //     val exception = new ShardException("o noes")
    //     expect {
    //       one(shard1).shardInfo willReturn shard1Info
    //       one(shard2).shardInfo willReturn shard1Info
    //       one(shard1).getName.willThrow(exception)
    //       one(shard2).getName.willThrow(exception)
    //     }
    //     replicatingShard.getName must throwA[ShardException]
    //   }
    // }
    // 
    // "writes happen to all shards" in {
    //   "when they succeed" in {
    //     expect {
    //       one(shard1).setName("alice")
    //       one(shard2).setName("alice")
    //     }
    //     replicatingShard.setName("alice")
    //   }
    // 
    //   "when the first one fails" in {
    //     expect {
    //       one(shard1).setName("alice").willThrow(new ShardException("o noes"))
    //       one(shard2).setName("alice")
    //     }
    //     replicatingShard.setName("alice") must throwA[Exception]
    //   }
    // }
    // 
    // "reads happen to shards in order" in {
    //   expect {
    //     one(shard1).getName.willReturn("ted")
    //   }
    //   replicatingShard.getName mustEqual "ted"
    // }
  }
}
