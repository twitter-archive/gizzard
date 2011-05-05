package com.twitter.gizzard
package proxy

import scala.reflect.Manifest
import com.twitter.util.{Duration, Time}
import com.twitter.gizzard.Stats
import com.twitter.ostrich.stats.{StatsProvider, TransactionalStatsCollection}
import scheduler.JsonJob


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a transactional logger.
 */
object LoggingProxy {
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
    consumers: Seq[TransactionalStatsConsumer], name: String, obj: T)(implicit manifest: Manifest[T]): T = {
    Proxy(obj) { method =>
      Stats.beginTransaction()
      val startTime = Time.now
      try {
        method()
      } catch {
        case e: Exception =>
          Stats.transaction.record("Request failed with exception: " + e.toString())
          Stats.transaction.set("exception", e)
          throw e
      } finally {
        val duration = Time.now - startTime
        Stats.transaction.record("Total duration: "+duration.inMillis)
        Stats.transaction.set("duration", duration.inMillis.asInstanceOf[AnyRef])
        val t = Stats.endTransaction()

        if (t.tags.size > 0) {
          val opName = t.tags.toSeq.sorted.mkString(",")
          Stats.global.incr("operation-"+opName+"-count")
          Stats.global.addMetric("operation-"+opName, duration.inMilliseconds.toInt)
        }

        consumers.map { _(t) }
      }
    }
  }
}

class JobLoggingProxy[T <: JsonJob](
  consumers: Seq[TransactionalStatsConsumer], name: String)(implicit manifest: Manifest[T]) {
  private val proxy = new ProxyFactory[T]

  def apply(job: T): T = {
    proxy(job) { method =>
      val (rv, t) = Stats.withTransaction { method() }
      consumers.map { _(t) }
      rv
    }
  }
}
