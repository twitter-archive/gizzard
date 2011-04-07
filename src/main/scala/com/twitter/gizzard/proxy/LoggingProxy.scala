package com.twitter.gizzard
package proxy

import scala.reflect.Manifest
import com.twitter.util.{Duration, Time}
import com.twitter.gizzard.Stats
import com.twitter.ostrich.stats.{StatsProvider, TransactionalStatsCollection}
import java.util.Random
import scheduler.JsonJob


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a transactional logger.
 */
object LoggingProxy {
  val rand = new Random

  /*def apply[T <: AnyRef](stats: StatsProvider, logger: TransactionalStatsCollection, name: String, methods: Set[String], obj: T)(implicit manifest: Manifest[T]): T = {
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
  } */

  def apply[T <: AnyRef](
    slowQueryLogger: TransactionalStatsCollection, slowQueryDuration: Duration,
    sampledQueryLogger: TransactionalStatsCollection, sampledQueryRate: Double,
    name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      val ((rv, duration), summary) = Stats.withTransaction {
        Stats.transaction.setLabel("timestamp", Time.now.inMillis.toString)
        Stats.transaction.setLabel("name", name)
        Stats.transaction.setLabel("method", method.name)
        if (method.args != null) {
          val names = method.argumentNames
          val zipped = if (names.length > 0) method.args.zip(names) else method.args.zipWithIndex
          zipped.foreach { case (value, name) => Stats.transaction.setLabel("argument/"+name, value.toString) }
        }
        val (rv, duration) = Duration.inMilliseconds { method() }
        Stats.transaction.addMetric("duration", duration.inMilliseconds.toInt)
        Stats.global.addMetric(method+"_timing", duration.inMilliseconds.toInt)
        (rv, duration)
      }
      if (duration >= slowQueryDuration) slowQueryLogger.write(summary)
      val i = rand.nextFloat()
      if (i < sampledQueryRate) sampledQueryLogger.write(summary)

      rv
    }
  }
}

class JobLoggingProxy[T <: JsonJob](
  slowQueryLogger: TransactionalStatsCollection, slowQueryThreshold: Duration,
  sampledQueryLogger: TransactionalStatsCollection, sampledQueryRate: Double)(implicit manifest: Manifest[T]) {
  private val proxy = new ProxyFactory[T]

  def apply(job: T): T = {
    proxy(job) { method =>
      val ((rv, duration), summary) = Stats.withTransaction {
        Stats.transaction.setLabel("timestamp", Time.now.inMillis.toString)
        Stats.transaction.setLabel("name", job.loggingName)
        Stats.transaction.setLabel("job", job.toJson)
        val (rv, duration) = Duration.inMilliseconds { method() }
        Stats.transaction.addMetric("duration", duration.inMilliseconds.toInt)
        Stats.global.addMetric(job.loggingName+"_timing", duration.inMilliseconds.toInt)
        (rv, duration)
      }
      if (duration >= slowQueryThreshold) slowQueryLogger.write(summary)
      val i = LoggingProxy.rand.nextFloat()
      if (i < sampledQueryRate) sampledQueryLogger.write(summary)
      rv
    }
  }
}
