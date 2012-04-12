package com.twitter.gizzard.nameserver

import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import scala.collection.mutable
import scala.util.Random
import java.util.concurrent.atomic.AtomicLong

import com.twitter.gizzard.ConfiguredSpecification

object LoadBalancerSpec extends ConfiguredSpecification with JMocker with ClassMocker {
  "LoadBalancer" should {
    val iterations = 1000000
    val tolerance = iterations * .01
    val in = List(1, 2, 3)

    "be random when weights are fixed" in {
      // all items have weight 1
      val histogram = histo(_ => 1)

      val avg = histogram.values.foldLeft(0L) { _ + _.get() } / histogram.size
      val sumOfSquares = histogram.values.map { i => (avg - i.get()) * (avg - i.get()) }.foldLeft(0L)(_ + _)
      val stdev = Math.sqrt(sumOfSquares / (histogram.size - 1))

      stdev must be_<(tolerance)
    }

    "drop 0-weight items" in {
      // exclude item
      val histogram = histo(i => if (i == 2) 0 else 1)
      histogram.keys must haveTheSameElementsAs(Seq(List(1, 3), List(3, 1)))
    }

    "be weighted" in {
      // items use themselves as weight
      val histogram = histo(i => i)
      val descending =
        histogram.toSeq.sorted {
          // TODO: use maxBy in Scala 2.9.x
          new math.Ordering[(List[Int], AtomicLong)] {
            def compare(x: (List[Int], AtomicLong), y: (List[Int], AtomicLong)) =
              y._2.get.compareTo(x._2.get)
          }
        }.map(_._1)
      descending.head must beEqualTo(List(3, 2, 1))
      descending.last must beEqualTo(List(1, 2, 3))
    }

    def histo(selector: LoadBalancer.WeightSelector[Int]) = {
      val histogram = mutable.HashMap[List[Int], AtomicLong]()
      val start = System.currentTimeMillis
      1.to(iterations).foreach { i =>
        val result = LoadBalancer.WeightedRandom.shuffle(selector, in)
        histogram.getOrElseUpdate(result.toList, new AtomicLong()).incrementAndGet()
      }
      histogram
    }
  }
}
