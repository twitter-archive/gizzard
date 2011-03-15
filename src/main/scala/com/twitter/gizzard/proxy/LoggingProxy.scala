package com.twitter.gizzard
package proxy

import scala.reflect.Manifest
import com.twitter.util.{Duration, Time}
import com.twitter.ostrich.stats.{Stats, StatsProvider, W3CStats}


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
        logger { tstats =>
        //logger.transaction {
          tstats.setLabel("timestamp", Time.now.inMillis.toString)
          tstats.setLabel("operation", name + ":" + method.name)
          val arguments = (if (method.args != null) method.args.mkString(",") else "").replaceAll("[ \n]", "_")
          tstats.setLabel("arguments", if (arguments.length < 200) arguments else (arguments.substring(0, 200) + "..."))
          val (rv, duration) = Duration.inMilliseconds { method() }
          tstats.addMetric("action-timing", duration.inMilliseconds.toInt)
          tstats.addMetric("x-operation-" + shortName + ":" + method.name, duration.inMilliseconds.toInt)

          if (rv != null) {
            // structural types don't appear to work for some reason.
            val resultCount = rv match {
              case col: Collection[_]               => col.size.toString
              case javaCol: java.util.Collection[_] => javaCol.size.toString
              case arr: Array[AnyRef]               => arr.size.toString
              case _: AnyRef                        => "1"
            }
            tstats.setLabel("result-count", resultCount)
          }
          rv
        }
      } else {
        method()
      }
    }
  }
}
