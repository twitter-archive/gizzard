package com.twitter.gizzard

import com.twitter.ostrich.stats.{DevNullStats, StatsCollection, Stats => OStats, StatsSummary}

object Stats {
  val global = OStats
  val internal = OStats.make("internal")

  def transaction = {
    val t = tl.get()
    if (t == null) DevNullStats else t
  }

  def beginTransaction() {
    setTransaction(new StatsCollection)
  }

  def endTransaction() {
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
