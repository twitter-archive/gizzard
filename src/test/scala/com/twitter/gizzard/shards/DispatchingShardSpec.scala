package com.twitter.gizzard.shards

import org.specs.mock.{ClassMocker, JMocker}

class DispatchingShardSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  val s1 = mock[fake.Shard]
  val s2 = mock[fake.Shard]
  val f1  = Forwarding(Address(0, 1), s1)
  val f2 = Forwarding(Address(0, 15), s2)
  val table = new ForwardingTable(List(f1, f2))
  val shard = new fake.ReadWriteShardAdapter(new DispatchingShard(new fake.ShardInfo, 1, table))
  
  "DispatchingShard" should {    
    "dispatch based upon the forwarding table" in {
      expect {
        one(s1).get("abc") willReturn Some("def")
        one(s2).get("hi") willReturn Some("world")
      }
      shard.get("hi") mustEqual Some("world")
      shard.get("abc") mustEqual Some("def")
    }
  }
}