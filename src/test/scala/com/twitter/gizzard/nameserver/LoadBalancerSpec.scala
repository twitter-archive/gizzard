package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.mutable
import scala.util.Random
import java.util.concurrent.atomic.AtomicLong

import com.twitter.gizzard.shards.RoutingNode
import com.twitter.gizzard.ConfiguredSpecification

object LoadBalancerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "LoadBalancer" should {
    val shard1 = mock[RoutingNode[AnyRef]]
    val shard2 = mock[RoutingNode[AnyRef]]
    val shard3 = mock[RoutingNode[AnyRef]]


    "actually be random" in {
      val iterations = 1000000
      val tolerance = iterations * .01
      expect {
        allowing(shard1).weight willReturn 1
        allowing(shard2).weight willReturn 1
        allowing(shard3).weight willReturn 1
      }

      val histogram = mutable.HashMap[List[RoutingNode[AnyRef]], AtomicLong]()
      1.to(iterations).foreach { i =>
        val loadBalancer = new LoadBalancer(new Random, List(shard1, shard2, shard3))
        val result = loadBalancer().toList
        histogram.getOrElseUpdate(result, new AtomicLong()).incrementAndGet()
      }

      val avg = histogram.values.foldLeft(0L) { _ + _.get() } / histogram.size
      val sumOfSquares = histogram.values.map { i => (avg - i.get()) * (avg - i.get()) }.foldLeft(0L)(_ + _)
      val stdev = Math.sqrt(sumOfSquares / (histogram.size - 1))

      stdev must be_<(tolerance)
    }
  }
}
