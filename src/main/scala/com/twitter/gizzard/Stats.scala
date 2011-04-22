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
      case None => setTransaction(new TransactionalStatsCollection(0L))
      case Some(t) => t.clearAll()
    }
  }

  def endTransaction() {
    transaction.clearAll()
    tl.set(null)
  }

  def setTransaction(collection: TransactionalStatsProvider) {
    tl.set(collection)
  }

  def withTransaction[T <: Any](f: => T): (T, TransactionalStatsProvider) = {
    beginTransaction()
    val rv = f
    val t = transaction
    endTransaction()
    (rv, t)
  }

  private val tl = new ThreadLocal[TransactionalStatsProvider]
}

case class TraceRecord(id: Long, timestamp: Time, message: String)

trait TransactionalStatsProvider {
  def record(message: => String)
  def toSeq: Seq[TraceRecord]
  def createChild(): TransactionalStatsProvider
  def children: Seq[TransactionalStatsProvider]
  def id: Long
  def clearAll()
}

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
    buf.append("  Children:\n")
    t.children.map { child =>
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

class SampledTransactionalStatsConsumer(consumer: TransactionalStatsConsumer, sampleRate: Double)
  extends TransactionalStatsConsumer {
  def apply(t: TransactionalStatsProvider) {
    val x = SampledTransactionalStatsConsumer.rng.nextFloat()
    if (x < sampleRate) consumer(t)
  }
}

class TransactionalStatsCollection(val id: Long) extends TransactionalStatsProvider {
  private val messages = new mutable.ArrayBuffer[TraceRecord]()
  private val childs = new mutable.ArrayBuffer[TransactionalStatsCollection]()

  def record(message: => String) {
    messages += TraceRecord(0L, Time.now, message)
  }

  def toSeq = messages.toSeq
  def children = childs.toSeq

  def createChild() = {
    val rv = new TransactionalStatsCollection(id)
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
}

/*
class TransactionalStatsCollection {
  private val collection: mutable.Map[String, Any]

  def setLabel(name: String, value: String) {
    collection(name) = value
  }


  def toMap: Map[String, Any] = collection.toMap
}*/
