package com.twitter.gizzard

import com.twitter.ostrich.stats.{DevNullStats, StatsCollection, Stats => OStats, StatsSummary, StatsProvider}

object Stats {
  val global = OStats
  val internal = new StatsCollection

  def transaction: StatsProvider = {
    val t = tl.get()
    if (t == null) DevNullStats else t
  }

  def beginTransaction() {
    transaction match {
      case DevNullStats => setTransaction(new StatsCollection)
      case t: StatsCollection => t.clearAll()
    }
  }

  def endTransaction() {
    transaction.clearAll()
    tl.set(null)
  }

  def setTransaction(collection: StatsCollection) {
    tl.set(collection)
  }

  def withTransaction[T <: Any](f: => T): (T, StatsSummary) = {
    beginTransaction()
    val rv = f
    val summary = transaction.get()
    endTransaction()
    (rv, summary)
  }

  private val tl = new ThreadLocal[StatsCollection]
}
/*
class TransactionalStatsCollection {
  private val collection: mutable.Map[String, Any]

  def setLabel(name: String, value: String) {
    collection(name) = value
  }


  def toMap: Map[String, Any] = collection.toMap
}*/
