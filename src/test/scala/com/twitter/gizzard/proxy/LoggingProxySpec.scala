package com.twitter.gizzard.proxy

import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}
import com.twitter.ostrich.DevNullStats


object LoggingProxySpec extends ConfiguredSpecification with JMocker with ClassMocker {
  trait Named {
    def name: String
  }

  trait Namer {
    def setName(name: String)
  }

  "LoggingProxy" should {
    val logger = mock[Logger]
    val bob = new Named { def name = "bob" }
    val rob = new Namer {
      def setName(name: String) {}
    }

    val w3cStats = new W3CStats(logger, Array("operation", "arguments", "action-timing"))
    val bobProxy = LoggingProxy[Named](DevNullStats, w3cStats, "Bob", bob)
    val robProxy = LoggingProxy[Namer](DevNullStats, w3cStats, "Rob", rob)

    "log stats on a proxied object" in {
      val line = capturingParam[String]
      expect {
        one(logger).info(line.capture, any[Array[AnyRef]])
      }

      bobProxy.name mustEqual "bob"
      line.captured must include("Bob:name")
    }

    "replace spaces and newlines in arguments to w3c logs" in {
      val line = capturingParam[String]
      expect {
        one(logger).info(line.capture, any[Array[AnyRef]])
      }

      robProxy.setName("rob bob\nlob")
      line.captured must include("rob_bob_lob")
    }
  }
}
