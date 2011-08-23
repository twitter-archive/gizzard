package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import scala.util.Random

import com.twitter.gizzard.shards.RoutingNode
import com.twitter.gizzard.ConfiguredSpecification


// FIXME: these tests kinda suck in theory. Ideally, we'd test based on
//        a fuzzy expectation of the distribution of responses.


object LoadBalancerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "LoadBalancer" should {
    val random = new Random(0)
    val shard1 = mock[RoutingNode[AnyRef]]
    val shard2 = mock[RoutingNode[AnyRef]]
    val shard3 = mock[RoutingNode[AnyRef]]

    "with a zero weight" in {
      expect {
        allowing(shard1).weight willReturn 3
        allowing(shard2).weight willReturn 0
        allowing(shard3).weight willReturn 1
      }
      val loadBalancer = new LoadBalancer(random, List(shard1, shard2, shard3))
      loadBalancer() mustEqual List(shard1, shard3)
    }

    "with interesting weights" in {
      expect {
        allowing(shard1).weight willReturn 3
        allowing(shard2).weight willReturn 2
        allowing(shard3).weight willReturn 1
      }
      val loadBalancer = new LoadBalancer(random, List(shard1, shard2, shard3))
      loadBalancer() mustEqual List(shard1, shard2, shard3)
    }
  }
}
