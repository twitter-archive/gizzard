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

object FailingOverLoadBalancerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "FailingOverLoadBalancer" should {
    val random = new Random(0)
    val shard1 = mock[RoutingNode[AnyRef]]
    val shard2 = mock[RoutingNode[AnyRef]]
    val shard3 = mock[RoutingNode[AnyRef]]

    "returns 1 online followed by randomly ordered offlines followed by rest of onlines" in {
      expect {
        allowing(shard1).weight willReturn 1
        allowing(shard2).weight willReturn 1
        allowing(shard3).weight willReturn 0
      }

      val loadBalancer = new FailingOverLoadBalancer(random, List(shard1, shard2, shard3))
      loadBalancer() mustEqual List(shard2, shard3, shard1)
      loadBalancer() mustEqual List(shard2, shard3, shard1)
      loadBalancer() mustEqual List(shard2, shard3, shard1)
      loadBalancer() mustEqual List(shard1, shard3, shard2)
      loadBalancer() mustEqual List(shard2, shard3, shard1)
      loadBalancer() mustEqual List(shard1, shard3, shard2)
    }

    "puts the offline shard first some of the time" in {
      expect {
        allowing(shard1).weight willReturn 1
        allowing(shard2).weight willReturn 1
        allowing(shard3).weight willReturn 0
      }

      val loadBalancer = new FailingOverLoadBalancer(random, List(shard1, shard3))
      var offlineWasInFront = false

      for ( i <- 1 to 1000 )
        if ( loadBalancer().head == shard3 )
          offlineWasInFront = true

      offlineWasInFront mustEqual true
    }
  }
}
