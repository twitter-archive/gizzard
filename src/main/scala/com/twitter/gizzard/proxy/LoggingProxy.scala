package com.twitter.gizzard
package proxy

import scala.reflect.Manifest
import com.twitter.util.{Duration, Time}
import com.twitter.ostrich.stats.{Stats, StatsProvider, TransactionalStatsCollection}
import java.util.Random
import scheduler.JsonJob


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a transactional logger.
 */
object LoggingProxy {
  val rand = new Random

  def apply[T <: AnyRef](stats: StatsProvider, logger: TransactionalStatsCollection, name: String, obj: T)(implicit manifest: Manifest[T]): T =
    apply(stats, logger, name, Set(), obj)

  def apply[T <: AnyRef](stats: StatsProvider, logger: TransactionalStatsCollection, name: String, methods: Set[String], obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      if (methods.size == 0 || methods.contains(method.name)) {
        val shortName = if (name contains ',') ("multi:" + name.substring(name.lastIndexOf(',') + 1)) else name
        if (method.name != "apply") {
          stats.incr("operation-" + shortName + "-" + method.name + "-count")
        }
        logger { tstats =>
          tstats.setLabel("timestamp", Time.now.inMillis.toString)
          tstats.setLabel("operation", name + ":" + method.name)
          val arguments = (if (method.args != null) method.args.mkString(",") else "").replaceAll("[ \n]", "_")
          tstats.setLabel("arguments", if (arguments.length < 200) arguments else (arguments.substring(0, 200) + "..."))
          val (rv, duration) = Duration.inMilliseconds { method() }
          stats.addMetric("x-operation-" + shortName + ":" + method.name, duration.inMilliseconds.toInt)

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

  def apply[T <: AnyRef](stats: StatsProvider, slowQueryLogger: TransactionalStatsCollection, slowQueryDuration: Duration, sampledQueryLogger: TransactionalStatsCollection, sampledQueryRate: Double, name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      val (rv, duration) = Duration.inMilliseconds { method() }

      val boundStats = populateStats(duration, name, method) _
      if (duration >= slowQueryDuration) slowQueryLogger { boundStats(_) }

      val i = rand.nextFloat()
      if (i < sampledQueryRate) sampledQueryLogger { boundStats(_) }

      rv
    }
  }

  private def populateStats[T <: AnyRef](duration: Duration, name: String, method: Proxy.MethodCall[T])(stats: StatsProvider) {
    stats.setLabel("timestamp", Time.now.inMillis.toString)
    stats.setLabel("name", name)
    stats.setLabel("method", method.name)
    if (method.args != null) {
      val names = method.argumentNames
      val zipped = if (names.length > 0) method.args.zip(names) else method.args.zipWithIndex
      zipped.foreach { case (value, name) => stats.setLabel("argument/"+name, value.toString) }
    }
    stats.addMetric("duration", duration.inMilliseconds.toInt)
  }
}

class JobLoggingProxy[T <: JsonJob](implicit manifest: Manifest[T], sampledQueryLogger: TransactionalStatsCollection, sampledQueryRate: Double) {
  private val proxy = new ProxyFactory[T]

  def apply(stats: StatsProvider, job: T): T = {
    proxy(job) { method =>
      val (rv, duration) = Duration.inMilliseconds { method() }

      val i = LoggingProxy.rand.nextFloat()
      if (i < sampledQueryRate) sampledQueryLogger { tstats =>
        stats.setLabel("timestamp", Time.now.inMillis.toString)
        stats.setLabel("name", job.loggingName)
        stats.setLabel("job", job.toJson)
        stats.addMetric("duration", duration.inMilliseconds.toInt)
      }

      rv
    }
  }
}
