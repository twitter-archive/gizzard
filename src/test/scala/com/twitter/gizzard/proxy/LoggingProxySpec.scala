package com.twitter.gizzard.proxy

import com.twitter.logging.Logger
import com.twitter.util.TimeConversions._
import com.twitter.util.{Future, Promise}
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.gizzard.util.{Future => GizzardFuture}
import com.twitter.gizzard.{Stats, TransactionalStatsProvider, TransactionalStatsConsumer, SampledTransactionalStatsConsumer}
import com.twitter.gizzard.ConfiguredSpecification
import java.util.concurrent._


object LoggingProxySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  trait Named {
    def name: String
    def nameParts: Array[String]
    def namePartsSeq: Seq[String]
    def asyncName: Future[String]
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

  "Logging Proxy" should {
    val gFuture = new GizzardFuture("test", 1, 1, 1.second, 1.second)
    val executor = Executors.newSingleThreadExecutor()

    val bob = new Named {
      def name = {
        Stats.transaction.record("ack")
        "bob"
      }
      def nameParts = throw new Exception("yarrg!")
      def namePartsSeq = {
        Stats.transaction.record("before thread")
        val f = gFuture {
          Stats.transaction.record("in thread")
          Seq("bob", "marley")
        }
        f.get()
      }
      def asyncName = {
        val promise = new Promise[String]
        executor.submit(new Runnable {
          def run = {
            Thread.sleep(101)
            promise.setValue("bob")
          }
        })
        promise
      }
    }

    val rob = new Namer {
      def setName(name: String) {}
      def setNameSlow(name: String) { Thread.sleep(100) }
    }

    val sampledStats = new FakeTransactionalStatsConsumer
    val sampledLoggingConsumer = new SampledTransactionalStatsConsumer(sampledStats, 1)
    val bobProxyFactory = new LoggingProxy[Named](Seq(sampledLoggingConsumer), "request", None)
    val bobProxy = bobProxyFactory(bob)
    val robProxyFactory = new LoggingProxy[Namer](Seq(sampledLoggingConsumer), "request", None)
    val robProxy = robProxyFactory(rob)

    doAfter {
      sampledStats.reset
    }

    "log a trace" in {
      bobProxy.name
      val messages = sampledStats.stats.toSeq.map { _.message }
      messages(0) mustEqual "ack"
      messages(1) must startWith("Total duration:")
    }

    "log a trace async" in {
      bobProxy.asyncName.apply()
      Stats.transactionOpt mustEqual None
      val messages = sampledStats.stats.toSeq.map { _.message }
      messages(0) must startWith("Total duration:")
      sampledStats.stats.get("duration").get.asInstanceOf[Long] must beGreaterThan(100L)
    }

    "log a trace across threads" in {
      bobProxy.namePartsSeq
      val messages = sampledStats.stats.toSeq.map { _.message }
      messages(0) mustEqual "before thread"
      messages(1) must startWith("Total duration:")
      val children = sampledStats.stats.children.map { _.toSeq.map { _.message } }
      children(0)(0) must startWith("Time spent in future queue")
      children(0)(1) mustEqual "in thread"
      children(0)(2) must startWith("Total duration:")
    }

    "log exceptions" in {
      bobProxy.nameParts must throwA[Exception]
      val messages = sampledStats.stats.toSeq.map { _.message }
      messages(0) mustEqual "Transaction failed with exception: java.lang.Exception: yarrg!"
      messages(1) must startWith("Total duration:")
    }
  }
}
