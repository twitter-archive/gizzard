package com.twitter.gizzard.proxy

import scala.reflect.Manifest
import com.twitter.xrayspecs.Time
import com.twitter.ostrich.{Stats, StatsProvider, W3CStats}


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a W3CStats logger.
 */
object LoggingProxy {
  def apply[T <: AnyRef](stats: StatsProvider, logger: W3CStats, name: String, obj: T)(implicit manifest: Manifest[T]): T =
    apply(stats, logger, name, Set(), obj)

  def apply[T <: AnyRef](stats: StatsProvider, logger: W3CStats, name: String, methods: Set[String], obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      if (methods.size == 0 || methods.contains(method.name)) {
        val shortName = if (name contains ',') ("multi:" + name.substring(name.lastIndexOf(',') + 1)) else name
        if (method.name != "apply") {
          stats.incr("operation-" + shortName + "-" + method.name + "-count")
        }
        logger.transaction {
          logger.log("timestamp", Time.now.inMillis)
          logger.log("operation", name + ":" + method.name)
          val arguments = (if (method.args != null) method.args.mkString(",") else "").replaceAll("[ \n]", "_")
          logger.log("arguments", if (arguments.length < 200) arguments else (arguments.substring(0, 200) + "..."))
          val (rv, msec) = Stats.duration { method() }
          logger.addTiming("action-timing", msec.toInt)
          stats.addTiming("x-operation-" + shortName + ":" + method.name, msec.toInt)

          if (rv != null) {
            // structural types don't appear to work for some reason.
            rv match {
              case col: Collection[_] => logger.log("result-count", col.size)
              case javaCol: java.util.Collection[_] => logger.log("result-count", javaCol.size)
              case arr: Array[AnyRef] => logger.log("result-count", arr.size)
              case _: AnyRef => logger.log("result-count", 1)
            }
          }
          rv
        }
      } else {
        method()
      }
    }
  }
}
