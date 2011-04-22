package com.twitter.gizzard
package proxy

import com.twitter.ostrich.stats.{TransactionalStatsCollection, StatsSummary}
import com.twitter.logging.Logger
import com.twitter.util.TimeConversions._
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object LoggingProxySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  trait Named {
    def name: String
    def nameParts: Array[String]
    def namePartsSeq: Seq[String]
  }

  trait Namer {
    def setName(name: String)
  }

  class FakeTransactionalStatsConsumer extends TransactionalStatsConsumer {
    var stats: TransactionalStatsProvider = null
    def apply(s: TransactionalStatsProvider) {
      stats = s
    }

    def reset {
      stats = null
    }
  }

/*  "LoggingProxy" should {
    val bob = new Named {
      def name = "bob"
      def nameParts = Seq("bob", "marley").toArray
      def namePartsSeq = Seq("bob", "marley")
    }
    val rob = new Namer {
      def setName(name: String) {}
    }

    val stats = new FakeLogger
    val bobProxy = LoggingProxy[Named](stats, "Bob", bob)
    val filteredBobProxy = LoggingProxy[Named](stats, "Bob", Set("name"), bob)
    val robProxy = LoggingProxy[Namer](stats, "Rob", rob)

    doAfter {
      stats.reset
    }

    "log stats on a proxied object" in {
      bobProxy.name mustEqual "bob"
      stats.summary.labels("operation") mustEqual "Bob:name"
    }

    "log the size of a result set" >> {
      "when the method returns nothing" >> {
        robProxy.setName("rob bob")
        stats.summary.labels.contains("result-count") mustBe false
      }

      "when the method returns a ref" >> {
        bobProxy.name mustEqual "bob"
        stats.summary.labels.contains("result-count") mustBe true
        stats.summary.labels("result-count").toInt mustEqual 1
      }

      "when the method returns an array" >> {
        bobProxy.nameParts.toList mustEqual List("bob", "marley")
        stats.summary.labels.contains("result-count") mustBe true
        stats.summary.labels("result-count").toInt mustEqual 2
      }

      "when the method returns a seq" >> {
        bobProxy.nameParts.toList mustEqual List("bob", "marley")
        stats.summary.labels.contains("result-count") mustBe true
        stats.summary.labels("result-count").toInt mustEqual 2
      }
    }

    "only logs methods from the specified set" in {
      filteredBobProxy.name
      filteredBobProxy.nameParts

      stats.summary.labels.contains("operation") mustBe true
      stats.summary.labels("operation") must include("Bob:name")
      stats.summary.labels("operation") mustNot include("Bob:nameParts")
    }
  } */

  "New School Logging Proxy" should {
    val future = new Future("test", 1, 1, 1.second, 1.second)

    val bob = new Named {
      def name = {
        Stats.transaction.record("ack")
        "bob"
      }
      def nameParts = Seq("bob", "marley").toArray
      def namePartsSeq = {
        Stats.transaction.record("before thread")
        val f = future {
          Stats.transaction.record("in thread")
          Seq("bob", "marley")
        }
        f.get()
      }
    }

    val rob = new Namer {
      def setName(name: String) {}
      def setNameSlow(name: String) { Thread.sleep(100) }
    }

    val sampledStats = new FakeTransactionalStatsConsumer
    val sampledLoggingConsumer = new SampledTransactionalStatsConsumer(sampledStats, 1)
//    val slowStats = new FakeLogger
//    val slowDuration = 5.millis
//    val sampledStats = new FakeLogger
//    val sampledRate = 1
    val bobProxy = LoggingProxy[Named](Seq(sampledLoggingConsumer), "test", bob)
    val robProxy = LoggingProxy[Namer](Seq(sampledLoggingConsumer), "test", rob)

    doAfter {
//      slowStats.reset
      sampledStats.reset
    }

    "log a trace" in {
      bobProxy.name
      sampledStats.stats.toSeq.map { _.message } mustEqual List("ack")
    }

    "log a trace across threads" in {
      bobProxy.namePartsSeq
      sampledStats.stats.toSeq.map { _.message } mustEqual List("before thread")
      sampledStats.stats.children.map { _.toSeq.map { _.message } } mustEqual List(List("in thread"))
    }
  }
}
