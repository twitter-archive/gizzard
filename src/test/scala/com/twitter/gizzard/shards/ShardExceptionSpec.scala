package com.twitter.gizzard.shards

import com.twitter.conversions.time._
import org.specs.Specification
import org.specs.mock.JMocker
import com.twitter.logging.{Logger, StringHandler, BareFormatter}
import com.twitter.gizzard.ConfiguredSpecification


object ShardExceptionSpec extends ConfiguredSpecification with JMocker {
  "ShardException" should {
    val handler = new StringHandler(BareFormatter, None)
    val log = Logger.get("shard_exception_spec")
    log.addHandler(handler)

    "not log the stack traces of timeouts" in {
      "normal" in {
        try {
          throw new ShardTimeoutException(100.milliseconds, ShardId("localhost", "table1"), null)
        } catch {
          case e =>
            log.error(e, "Aie! " + e)
        }

        handler.get.split("\n").toList mustEqual
          List("Aie! com.twitter.gizzard.shards.ShardTimeoutException: Timeout (100 msec): localhost/table1")
      }

      "with cause" in {
        try {
          try {
            throw new Exception("holy cow!")
          } catch {
            case nested =>
              throw new ShardTimeoutException(100.milliseconds, ShardId("localhost", "table1"), nested)
          }
        } catch {
          case e =>
            log.error(e, "Aie! " + e)
        }

        handler.get.split("\n").toList mustEqual
          List("Aie! com.twitter.gizzard.shards.ShardTimeoutException: Timeout (100 msec): localhost/table1")
      }
    }
  }
}
