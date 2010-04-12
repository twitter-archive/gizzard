package com.twitter.gizzard.proxy

import scala.reflect.Manifest
import com.twitter.ostrich.{Stats, StatsProvider, W3CStats}


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a W3CStats logger.
 */
object LoggingProxy {
  var counter = 0

  def apply[T <: AnyRef](stats: StatsProvider, logger: W3CStats, name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      val shortName = if (name contains ',') ("multi:" + name.substring(name.lastIndexOf(',') + 1)) else name
      stats.incr("operation-" + shortName + ":" + method.name)
      logger.transaction {
        logger.log("operation", name + ":" + method.name)
        val arguments = (if (method.args != null) method.args.mkString(",") else "").replace(' ', '_')
        logger.log("arguments", if (arguments.length < 200) arguments else (arguments.substring(0, 200) + "..."))
        val (rv, msec) = Stats.duration { method() }
        logger.addTiming("action-timing", msec.toInt)
        stats.addTiming("x-operation-" + shortName + ":" + method.name, msec.toInt)
        rv
      }
    }
  }
}
