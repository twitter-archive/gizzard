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
          Stats.global.incr(name+"-request-failed")
          Stats.transaction.name.foreach { opName =>
            Stats.global.incr("operation-"+opName+"-failed")
          }
          throw e
      } finally {
        Stats.global.incr(name+"-request-total")

        val duration = Time.now - startTime
        Stats.transaction.record("Total duration: "+duration.inMillis)
        Stats.transaction.set("duration", duration.inMillis.asInstanceOf[AnyRef])

        Stats.transaction.name.foreach { opName =>
          Stats.global.incr("operation-"+opName+"-count")
          Stats.global.addMetric("operation-"+opName, duration.inMilliseconds.toInt)
        }

        val t = Stats.endTransaction()
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
