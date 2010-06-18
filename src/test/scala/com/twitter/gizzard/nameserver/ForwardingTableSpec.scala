package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}

object ForwardingTableSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "ForwardingTable" should {
    val s0 = mock[shards.Shard]
    val s1 = mock[shards.Shard]
    val s2 = mock[shards.Shard]
    val s3 = mock[shards.Shard]
    val s4 = mock[shards.Shard]
    expect {
      allowing(s3).children willReturn(List(s4))
    }
    
    val f1  = ConcreteForwarding(Address(0, 1), s1)
    val f1a = ConcreteForwarding(Address(0, 3), s1)
    val f2  = ConcreteForwarding(Address(0, 5), s2)
    val f3  = ConcreteForwarding(Address(0, 10), s3)
    val table = new ForwardingTable(List(f1, f2, f3))

    "#forwardingsForShard" in {
      "returns the simple forwardings" in {
        table.forwardingsForShard(s1) mustEqual List(f1, f1a)
        table.forwardingsForShard(s2) mustEqual List(f2)
      }
      
      "returns forwardings for deeper shards" in {
        table.forwardingsForShard(s4) mustEqual List(f3)
      }
      
      "returns empty list for irrelevant shard" in {
        table.forwardingsForShard(s4) mustEqual List(s0)
      }
    }
    
    "#getShard" in {    
      "returns the shard for the forwarding range whose baseid is the largest base id less than or equal to the requested address" in {
        "when less than" in {
          table.getShard(Address(0, 2)) mustEqual s1
          table.getShard(Address(0, 4)) mustEqual s1
          table.getShard(Address(0, 7)) mustEqual s2
          table.getShard(Address(0, 11)) mustEqual s3
        }
        
        "when equal to" in {
          table.getShard(Address(0, 1)) mustEqual s1
          table.getShard(Address(0, 3)) mustEqual s1
          table.getShard(Address(0, 5)) mustEqual s2
          table.getShard(Address(0, 10)) mustEqual s3
        }
      } 
       
      "throws exception if no matching forwarding range" in {
        table.getShard(Address(0, 0)) must throwA[AddressOutOfBounds]
      }
    }    
  }
}
