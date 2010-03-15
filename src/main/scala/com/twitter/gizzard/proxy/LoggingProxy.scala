package com.twitter.gizzard.proxy

import scala.reflect.Manifest
import com.twitter.ostrich.{StatsProvider, W3CStats}


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a W3CStats logger.
 */
object LoggingProxy {
  var counter = 0

  def apply[T <: AnyRef](stats: Option[StatsProvider], logger: W3CStats, name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      val shortName = name.lastIndexOf('.') match {
        case -1 => name
        case n => name.substring(n + 1)
      }

      stats.map { _.incr("x-operation-" + shortName + ":" + method.name) }
      logger.transaction {
        logger.log("operation", shortName + ":" + method.name)
        val arguments = (if (method.args != null) method.args.mkString(",") else "").replace(' ', '_')
        logger.log("arguments", if (arguments.length < 200) arguments else (arguments.substring(0, 200) + "..."))
        logger.time("action-timing") {
          method()
        }
      }
    }
  }
}
