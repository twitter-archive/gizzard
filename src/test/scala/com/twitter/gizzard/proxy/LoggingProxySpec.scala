package com.twitter.gizzard
package proxy

import com.twitter.ostrich.stats.{TransactionalStatsCollection, StatsSummary, DevNullStats}
import com.twitter.logging.Logger
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

  class FakeLogger extends TransactionalStatsCollection {
    var summary: StatsSummary = null
    def write(s: StatsSummary) {
      summary = s
    }

    def reset {
      summary = null
    }
  }

  "LoggingProxy" should {
    val bob = new Named {
      def name = "bob"
      def nameParts = Seq("bob", "marley").toArray
      def namePartsSeq = Seq("bob", "marley")
    }
    val rob = new Namer {
      def setName(name: String) {}
    }

    val stats = new FakeLogger
    val bobProxy = LoggingProxy[Named](DevNullStats, stats, "Bob", bob)
    val filteredBobProxy = LoggingProxy[Named](DevNullStats, stats, "Bob", Set("name"), bob)
    val robProxy = LoggingProxy[Namer](DevNullStats, stats, "Rob", rob)

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
  }
}
