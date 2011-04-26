package com.twitter.gizzard

import com.twitter.ostrich.stats.{DevNullStats, StatsCollection, Stats => OStats, StatsSummary, StatsProvider, Counter, Metric}
import com.twitter.logging.Logger
import com.twitter.util.Time
import scala.collection.mutable
import java.util.Random

object Stats {
  val global = OStats
  val internal = new StatsCollection

  def transaction: TransactionalStatsProvider = {
    val t = tl.get()
    if (t == null) DevNullTransactionalStats else t
  }

  def transactionOpt = {
    val t = tl.get()
    if (t == null) None else Some(t)
  }

  def beginTransaction() {
    transactionOpt match {
      case None => setTransaction(new TransactionalStatsCollection(rng.nextInt(Integer.MAX_VALUE)))
      case Some(t) => t.clearAll()
    }
  }

  def endTransaction() = {
    val old = tl.get()
    tl.set(null)
    old
  }

  def setTransaction(collection: TransactionalStatsProvider) {
    tl.set(collection)
  }

  def withTransaction[T <: Any](f: => T): (T, TransactionalStatsProvider) = {
    beginTransaction()
    val rv = f
    val t = endTransaction()
    (rv, t)
  }

  private val tl = new ThreadLocal[TransactionalStatsProvider]
  private val rng = new Random
}

case class TraceRecord(id: Long, timestamp: Time, message: String)

trait TransactionalStatsConsumer {
  def apply(t: TransactionalStatsProvider)
}

class LoggingTransactionalStatsConsumer(log: Logger) extends TransactionalStatsConsumer {
  def apply(t: TransactionalStatsProvider) {
    val buf = new StringBuilder

    buf.append("Trace "+t.id+"\n")
    t.toSeq.map { record =>
       buf.append("  ["+record.timestamp.inMillis+"] "+record.message+"\n")
    }
    t.children.map { child =>
      buf.append("  Child Thread "+child.id+":\n")
      child.toSeq.map { record =>
        buf.append("    ["+record.timestamp.inMillis+"] "+record.message+"\n")
      }
    }
    log.info(buf.toString)
  }
}

object SampledTransactionalStatsConsumer {
  val rng = new Random
}

abstract class ConditionalTransactionalStatsConsumer(
  consumer: TransactionalStatsConsumer,
  f: TransactionalStatsProvider => Boolean) extends TransactionalStatsConsumer {
  def apply(t: TransactionalStatsProvider) {
    if (f(t)) consumer(t)
  }
}

class SampledTransactionalStatsConsumer(consumer: TransactionalStatsConsumer, sampleRate: Double)
  extends ConditionalTransactionalStatsConsumer(consumer, { t =>
    SampledTransactionalStatsConsumer.rng.nextFloat() < sampleRate
  })

class SlowTransactionalStatsConsumer(consumer: TransactionalStatsConsumer, threshold: Long)
  extends ConditionalTransactionalStatsConsumer(consumer, { t =>
    t.get("duration").map { _.asInstanceOf[Long] > threshold }.getOrElse(false)
  })

trait TransactionalStatsProvider {
  def record(message: => String)
  def set(key: String, value: AnyRef)
  def get(key: String): Option[AnyRef]
  def toSeq: Seq[TraceRecord]
  def createChild(): TransactionalStatsProvider
  def children: Seq[TransactionalStatsProvider]
  def id: Long
  def clearAll()
}

class TransactionalStatsCollection(val id: Long) extends TransactionalStatsProvider {
  private val messages = new mutable.ArrayBuffer[TraceRecord]()
  private val childs = new mutable.ArrayBuffer[TransactionalStatsCollection]()
  private val vars = new mutable.HashMap[String, AnyRef]

  def record(message: => String) {
    messages += TraceRecord(id, Time.now, message)
  }

  def set(key: String, value: AnyRef) { vars.put(key, value) }
  def get(key: String) = { vars.get(key) }

  def toSeq = messages.toSeq
  def children = childs.toSeq

  def createChild() = {
    val rv = new TransactionalStatsCollection(childs.size+1)
    childs += rv
    rv
  }

  def clearAll() {
    messages.clear()
    childs.clear()
  }
}

object DevNullTransactionalStats extends TransactionalStatsProvider {
  def clearAll() {}
  def record(message: => String) {}
  def toSeq = Seq()
  def createChild() = DevNullTransactionalStats
  def children = Seq()
  def id = 0L
  def set(key: String, value: AnyRef) {}
  def get(key: String) = None
}

/*
class TransactionalStatsCollection {
  private val collection: mutable.Map[String, Any]

  def setLabel(name: String, value: String) {
    collection(name) = value
  }


  def toMap: Map[String, Any] = collection.toMap
}*/
