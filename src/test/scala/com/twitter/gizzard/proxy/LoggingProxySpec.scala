package com.twitter.gizzard.proxy

import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.ostrich.DevNullStats


object LoggingProxySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  trait Named {
    def name: String
    def nameParts: Array[String]
    def namePartsSeq: Seq[String]
  }

  trait Namer {
    def setName(name: String)
  }

  "LoggingProxy" should {
    val logger = mock[Logger]
    val bob = new Named {
      def name = "bob"
      def nameParts = Seq("bob", "marley").toArray
      def namePartsSeq = Seq("bob", "marley")
    }
    val rob = new Namer {
      def setName(name: String) {}
    }

    val w3cFields = Array("operation", "arguments", "action-timing", "result-count")
    val w3cStats = new W3CStats(logger, w3cFields)
    val bobProxy = LoggingProxy[Named](DevNullStats, w3cStats, "Bob", bob)
    val filteredBobProxy = LoggingProxy[Named](DevNullStats, w3cStats, "Bob", Set("name"), bob)
    val robProxy = LoggingProxy[Namer](DevNullStats, w3cStats, "Rob", rob)

    "log stats on a proxied object" in {
      val line = capturingParam[String]
      expect {
        one(logger).info(line.capture, any[Array[AnyRef]])
      }

      bobProxy.name mustEqual "bob"
      line.captured must include("Bob:name")
    }

    "log the size of a result set" >> {
      val line = capturingParam[String]

      "when the method returns nothing" >> {
        expect {
          one(logger).info(line.capture, any[Array[AnyRef]])
        }

        robProxy.setName("rob bob")
        val fields = line.captured.split(" ")
        fields(w3cFields.indexOf("result-count")) mustEqual "-"
      }

      "when the method returns a ref" >> {
        expect {
          one(logger).info(line.capture, any[Array[AnyRef]])
        }

        bobProxy.name mustEqual "bob"
        val fields = line.captured.split(" ")
        fields(w3cFields.indexOf("result-count")).toInt mustEqual 1
      }

      "when the method returns an array" >> {
        expect {
          one(logger).info(line.capture, any[Array[AnyRef]])
        }

        bobProxy.nameParts.toList mustEqual List("bob", "marley")
        val fields = line.captured.split(" ")
        fields(w3cFields.indexOf("result-count")).toInt mustEqual 2
      }

      "when the method returns a seq" >> {
        expect {
          one(logger).info(line.capture, any[Array[AnyRef]])
        }

        bobProxy.nameParts.toList mustEqual List("bob", "marley")
        val fields = line.captured.split(" ")
        fields(w3cFields.indexOf("result-count")).toInt mustEqual 2
      }
    }

    "replace spaces and newlines in arguments to w3c logs" in {
      val line = capturingParam[String]
      expect {
        one(logger).info(line.capture, any[Array[AnyRef]])
      }

      robProxy.setName("rob bob\nlob")
      line.captured must include("rob_bob_lob")
    }

    "only logs methods from the specified set" in {
      val line = capturingParam[String]
      expect {
        one(logger).info(line.capture, any[Array[AnyRef]])
      }

      filteredBobProxy.name
      filteredBobProxy.nameParts

      line.captured must include("name")
      line.captured mustNot include("nameParts")
    }
  }
}
