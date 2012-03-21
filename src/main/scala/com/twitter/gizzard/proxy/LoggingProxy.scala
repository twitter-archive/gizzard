package com.twitter.gizzard.proxy

import scala.reflect.Manifest
import com.twitter.util.{Duration, Time, Future}
import com.twitter.gizzard.{Stats, TransactionalStatsConsumer}
import com.twitter.gizzard.scheduler.JsonJob


/**
 * Wrap an object's method calls in a logger that sends the method name, arguments, and elapsed
 * time to a transactional logger.
 *
 */
class LoggingProxy[T <: AnyRef](
  consumers: Seq[TransactionalStatsConsumer], statGrouping: String, name: Option[String])(implicit manifest: Manifest[T]) {

  private val proxy = new ProxyFactory[T]

  def apply(obj: T): T = {
    proxy(obj) { method =>
      Stats.beginTransaction()
      val namePrefix = name.map(_+"-").getOrElse("")
      val startTime = Time.now
      try {
        val ret = method()
        // TODO: The logic below and Stats' use of com.twitter.util.Local ensures our stats below
        // are updated correctly for async computations, but any stats inside method() wouldn't be.
        // Replace this eventually with a mechanism more suited to Finagle.
        ret match {
          case f: Future[_] => {
            f onSuccess { _ =>
              handleDone(namePrefix, startTime)
            } onFailure { throwable =>
              handleException(namePrefix, startTime, throwable)
            }
          }

          case _ => {
            handleDone(namePrefix, startTime)
            ret
          }
        }
      } catch {
        case t: Throwable =>
          handleException(namePrefix, startTime, t)
          throw t
      }
    }
  }

  def handleException(namePrefix: String, startTime: Time, t: Throwable) = {
    Stats.transaction.record("Transaction failed with exception: " + t.toString())
    Stats.transaction.set("exception", t)
    Stats.incr(namePrefix+statGrouping+"-failed")
    Stats.transaction.name.foreach { opName =>
      Stats.incr(namePrefix+"operation-"+opName+"-failed")
    }
    handleDone(namePrefix, startTime)
  }

  def handleDone(namePrefix: String, startTime: Time) = {
    Stats.incr(namePrefix+statGrouping+"-total")

    val duration = Time.now - startTime
    Stats.transaction.record("Total duration: "+duration.inMillis)
    Stats.transaction.set("duration", duration.inMillis.asInstanceOf[AnyRef])

    Stats.transaction.name.foreach { opName =>
      Stats.incr(namePrefix+"operation-"+opName+"-total")
      Stats.addMetric(namePrefix+"operation-"+opName, duration.inMilliseconds.toInt)
    }

    val t = Stats.endTransaction()
    consumers.map { _(t) }
  }
}
