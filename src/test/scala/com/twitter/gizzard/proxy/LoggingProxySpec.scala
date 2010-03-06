package com.twitter.gizzard.proxy

import com.twitter.ostrich.W3CStats
import net.lag.logging.Logger
import org.specs.Specification
import org.specs.mock.{ClassMocker, JMocker}


object LoggingProxySpec extends Specification with JMocker with ClassMocker {
  trait Named {
    def name: String
  }

  "LoggingProxy" should {
    "log stats on a proxied object" in {
      val logger = mock[Logger]
      val bob = new Named { def name = "bob" }
      val bobProxy = LoggingProxy[Named](new W3CStats(logger, Array("operation", "arguments", "action-timing")), "Bob", bob)

      val line = capturingParam[String]
      expect {
        one(logger).info(line.capture, any[Array[AnyRef]])
      }

      bobProxy.name mustEqual "bob"
      line.captured must include("Bob:name")
    }
  }
}
