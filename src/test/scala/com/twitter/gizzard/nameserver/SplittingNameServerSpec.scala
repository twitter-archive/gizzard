package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import shards._
import scala.collection.mutable

object SplittingNameServerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "SplittingNameServer" should {
    val original = mock[nameserver.NameServer[fake.Shard]]
    val shard = new fake.ConcreteShard()
    val child1 = new fake.ConcreteShard()
    val child2 = new fake.ConcreteShard()
    val a = Forwarding(1, 4, child1.shardId)
    val b = Forwarding(1, 10, child2.shardId)
    var forwardings = List(a, b)
    
    "reassign the original forwardings" in {
      expect {
        one(original).getForwardingsForShard(shard.shardId) willReturn forwardings
      }
      val nameServer = new SplittingNameServer(original, shard, List(child1, child2))
      nameServer.getForwardingsForShard(child1.shardId) mustEqual List(a)
      nameServer.getForwardingsForShard(child2.shardId) mustEqual List(b)
    }
    
    "throw an exception if children size not equal to forwarding count" in {
      expect {
        one(original).getForwardingsForShard(shard.shardId) willReturn forwardings
      }
      val bonusChild = new fake.ConcreteShard()  
      new SplittingNameServer(original, shard, List(child1, child2, bonusChild)) must throwA[InvalidNameServer]     
    }
  }
}