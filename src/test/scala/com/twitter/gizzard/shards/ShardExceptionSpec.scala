package com.twitter.gizzard.shards

import com.twitter.xrayspecs.TimeConversions._
import net.lag.logging.{GenericFormatter, Logger, StringHandler}
import org.specs.Specification
import org.specs.mock.JMocker

object ShardExceptionSpec extends ConfiguredSpecification with JMocker {
  "ShardException" should {
    val handler = new StringHandler(new GenericFormatter(""))
    val log = Logger.get("shard_exception_spec")
    log.addHandler(handler)

    "not log the stack traces of timeouts" in {
      "normal" in {
        try {
          throw new ShardTimeoutException(100.milliseconds, ShardId("localhost", "table1"), null)
        } catch {
          case e =>
            log.error(e, "Aie!")
        }

        handler.toString.split("\n").toList mustEqual
          List("Aie!", "com.twitter.gizzard.shards.ShardTimeoutException: Timeout (100 msec): localhost/table1")
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
            log.error(e, "Aie!")
        }

        handler.toString.split("\n").toList mustEqual
          List("Aie!", "com.twitter.gizzard.shards.ShardTimeoutException: Timeout (100 msec): localhost/table1")
      }
    }
  }
}
