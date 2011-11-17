/*
 * Copyright 2009 Twitter, Inc.
 * Copyright 2009 Robey Pointer <robeypointer@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.lag.kestrel

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.util.concurrent.CountDownLatch
import scala.collection.mutable
import com.twitter.actors.{Actor, TIMEOUT}
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.Logger
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Duration, Time}
import config._

class PersistentQueue(val name: String, persistencePath: String, @volatile var config: QueueConfig,
                      queueLookup: Option[(String => Option[PersistentQueue])]) {
  def this(name: String, persistencePath: String, config: QueueConfig) =
    this(name, persistencePath, config, None)

  private case class Waiter(actor: Actor)
  private case object ItemArrived

  private val log = Logger.get(getClass.getName)

  private val isFanout = (name contains '+')

  // current size of all data in the queue:
  private var queueSize: Long = 0

  // age of the last item read from the queue:
  private var _currentAge: Duration = 0.milliseconds

  // # of items EVER added to the queue:
  val totalItems = Stats.getCounter(name + "_total_items")
  totalItems.reset()

  // # of items that were expired by the time they were read:
  val totalExpired = Stats.getCounter(name + "_expired_items")
  totalExpired.reset()

  // # of items thot were discarded because the queue was full:
  val totalDiscarded = Stats.getCounter(name + "_discarded_items")
  totalDiscarded.reset()

  // # of items in the queue (including those not in memory)
  private var queueLength: Long = 0

  private var queue = new mutable.Queue[QItem]

  private var _memoryBytes: Long = 0

  private var closed = false
  private var paused = false

  // clients waiting on an item in this queue
  private val waiters = new mutable.ListBuffer[Waiter]

  private var journal =
    new Journal(new File(persistencePath).getCanonicalPath, name, config.syncJournal, config.multifileJournal)

  // track tentative removals
  private var xidCounter: Int = 0
  private val openTransactions = new mutable.HashMap[Int, QItem]
  private def openTransactionIds = openTransactions.keys.toSeq.sorted.reverse
  def openTransactionCount = synchronized { openTransactions.size }

  def length: Long = synchronized { queueLength }
  def bytes: Long = synchronized { queueSize }
  def journalSize: Long = synchronized { journal.size }
  def currentAge: Duration = synchronized { if (queueSize == 0) 0.milliseconds else _currentAge }
  def waiterCount: Long = synchronized { waiters.size }
  def isClosed: Boolean = synchronized { closed || paused }

  // mostly for unit tests.
  def memoryLength: Long = synchronized { queue.size }
  def memoryBytes: Long = synchronized { _memoryBytes }
  def inReadBehind = synchronized { journal.inReadBehind }

  if (!config.keepJournal) journal.erase()

  @volatile var expireQueue: Option[PersistentQueue] = config.expireToQueue.flatMap { name => queueLookup.flatMap(_(name)) }

  // FIXME
  def dumpConfig(): Array[String] = synchronized {
    Array(
      "max_items=" + config.maxItems,
      "max_size=" + config.maxSize,
      "max_age=" + config.maxAge,
      "max_journal_size=" + config.maxJournalSize.inBytes,
      "max_memory_size=" + config.maxMemorySize.inBytes,
      "max_journal_overflow=" + config.maxJournalOverflow,
      "discard_old_when_full=" + config.discardOldWhenFull,
      "journal=" + config.keepJournal,
      "sync_journal=" + config.syncJournal,
      "move_expired_to=" + config.expireToQueue.getOrElse("(none)")
    )
  }

  def dumpStats(): Array[(String, String)] = synchronized {
    Array(
      ("items", length.toString),
      ("bytes", bytes.toString),
      ("total_items", totalItems().toString),
      ("logsize", journalSize.toString),
      ("expired_items", totalExpired().toString),
      ("mem_items", memoryLength.toString),
      ("mem_bytes", memoryBytes.toString),
      ("age", currentAge.inMilliseconds.toString),
      ("discarded", totalDiscarded().toString),
      ("waiters", waiterCount.toString),
      ("open_transactions", openTransactionCount.toString)
    )
  }

  def gauge(gaugeName: String, value: => Double) = Stats.addGauge(name + "_" + gaugeName)(value)

  gauge("items", length)
  gauge("bytes", bytes)
  gauge("journal_size", journal.totalSize)
  gauge("mem_items", memoryLength)
  gauge("mem_bytes", memoryBytes)
  gauge("age_msec", currentAge.inMilliseconds)
  gauge("waiters", waiterCount)
  gauge("open_transactions", openTransactionCount)

  private final def adjustExpiry(startingTime: Time, expiry: Option[Time]): Option[Time] = {
    if (config.maxAge.isDefined) {
      val maxExpiry = startingTime + config.maxAge.get
      if (expiry.isDefined) Some(expiry.get min maxExpiry) else Some(maxExpiry)
    } else {
      expiry
    }
  }

  final def rollJournal() {
    if (config.keepJournal) {
      log.info("Rolling journal file for '%s' (qsize=%d)", name, queueSize)
      synchronized {
        if (config.multifileJournal) {
          journal.rotate()
        } else {
          journal.roll(xidCounter, openTransactionIds map { openTransactions(_) }, queue)
        }
      }
    }
  }

  final def checkRotateJournal() {
    if (config.keepJournal && config.multifileJournal && journal.size > config.maxJournalSize.inBytes) {
      synchronized {
        log.info("Rotating journal file for '%s'", name)
        journal.rotate()
      }
    }
  }

  /**
   * Add a value to the end of the queue, transactionally.
   */
   def add(value: Array[Byte], expiry: Option[Time], xid: Option[Int]): Boolean = synchronized {
    if (closed || value.size > config.maxItemSize.inBytes) return false
    if (config.fanoutOnly && !isFanout) return true
    while (queueLength >= config.maxItems || queueSize >= config.maxSize.inBytes) {
      if (!config.discardOldWhenFull) return false
      _remove(false)
      totalDiscarded.incr()
      if (config.keepJournal) journal.remove()
    }

    val now = Time.now
    val item = QItem(now, adjustExpiry(now, expiry), value, 0)
    if (config.keepJournal && !journal.inReadBehind) {
      if (journal.size > config.maxJournalSize.inBytes * config.maxJournalOverflow &&
          queueSize < config.maxJournalSize.inBytes) {
        // force re-creation of the journal.
        rollJournal()
      }
      if (queueSize >= config.maxMemorySize.inBytes) {
        log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
        journal.startReadBehind
      }
    }
    checkRotateJournal()
    if (xid ne None) openTransactions.remove(xid.get)
    _add(item)
    if (config.keepJournal) {
      xid match {
        case None => journal.add(item)
        case _    => journal.continue(xid.get, item)
      }
    }
    if (waiters.size > 0) {
      waiters.remove(0).actor ! ItemArrived
    }
    true
  }

  def add(value: Array[Byte]): Boolean = add(value, None, None)
  def add(value: Array[Byte], expiry: Option[Time]): Boolean = add(value, expiry, None)

  def continue(xid: Int, value: Array[Byte]): Boolean = add(value, None, Some(xid))
  def continue(xid: Int, value: Array[Byte], expiry: Option[Time]): Boolean = add(value, expiry, Some(xid))

  /**
   * Peek at the head item in the queue, if there is one.
   */
  def peek(): Option[QItem] = {
    synchronized {
      if (closed || paused || queueLength == 0) {
        None
      } else {
        _peek()
      }
    }
  }

  /**
   * Remove and return an item from the queue, if there is one.
   *
   * @param transaction true if this should be considered the first part
   *     of a transaction, to be committed or rolled back (put back at the
   *     head of the queue)
   */
  def remove(transaction: Boolean): Option[QItem] = {
    synchronized {
      if (closed || paused || queueLength == 0) {
        None
      } else {
        val item = _remove(transaction)
        if (config.keepJournal) {
          if (transaction) journal.removeTentative() else journal.remove()

          if ((queueLength == 0) && (journal.size >= config.maxJournalSize.inBytes)) {
            rollJournal()
          }
          checkRotateJournal()
        }
        item
      }
    }
  }

  /**
   * Remove and return an item from the queue, if there is one.
   */
  def remove(): Option[QItem] = remove(false)

  def operateReact(op: => Option[QItem], timeout: Option[Time])(f: Option[QItem] => Unit): Unit = {
    operateOrWait(op, timeout) match {
      case (item, None) =>
        f(item)
      case (None, Some(w)) =>
        val actorTimeout = if (timeout.isDefined) (timeout.get - Time.now).inMilliseconds else 0
        Actor.self.reactWithin(actorTimeout max 0) {
          case ItemArrived => operateReact(op, timeout)(f)
          case TIMEOUT => synchronized {
            waiters -= w
            // race: someone could have done an add() between the timeout and grabbing the lock.
            Actor.self.reactWithin(0) {
              case ItemArrived => f(op)
              case TIMEOUT => f(op)
            }
          }
        }
      case _ => throw new RuntimeException()
    }
  }

  def operateReceive(op: => Option[QItem], timeout: Option[Time]): Option[QItem] = {
    operateOrWait(op, timeout) match {
      case (item, None) =>
        item
      case (None, Some(w)) =>
        val actorTimeout = if (timeout.isDefined) (timeout.get - Time.now).inMilliseconds else 0
        val gotSomething = Actor.self.receiveWithin(actorTimeout max 0) {
          case ItemArrived => true
          case TIMEOUT => false
        }
        if (gotSomething) {
          operateReceive(op, timeout)
        } else {
          synchronized { waiters -= w }
          // race: someone could have done an add() between the timeout and grabbing the lock.
          Actor.self.receiveWithin(0) {
            case ItemArrived =>
            case TIMEOUT =>
          }
          op
        }
      case _ => throw new RuntimeException()
    }
  }

  def removeReact(timeout: Option[Time], transaction: Boolean)(f: Option[QItem] => Unit): Unit = {
    operateReact(remove(transaction), timeout)(f)
  }

  def removeReceive(timeout: Option[Time], transaction: Boolean): Option[QItem] = {
    operateReceive(remove(transaction), timeout)
  }

  def peekReact(timeout: Option[Time])(f: Option[QItem] => Unit): Unit = {
    operateReact(peek, timeout)(f)
  }

  def peekReceive(timeout: Option[Time]): Option[QItem] = {
    operateReceive(peek, timeout)
  }

  /**
   * Perform an operation on the next item from the queue, if there is one.
   * If the queue is closed, returns immediately. Otherwise, if a timeout is passed in, the
   * current actor is added to the wait-list, and will receive `ItemArrived` when an item is
   * available (or the queue is closed).
   */
  private def operateOrWait(op: => Option[QItem], timeout: Option[Time]): (Option[QItem], Option[Waiter]) = synchronized {
    val item = op
    if (!item.isDefined && !closed && !paused && timeout.isDefined) {
      val w = Waiter(Actor.self)
      waiters += w
      (None, Some(w))
    } else {
      (item, None)
    }
  }

  /**
   * Return a transactionally-removed item to the queue. This is a rolled-
   * back transaction.
   */
  def unremove(xid: Int): Unit = {
    synchronized {
      if (!closed) {
        if (config.keepJournal) journal.unremove(xid)
        _unremove(xid)
        if (waiters.size > 0) {
          waiters.remove(0).actor ! ItemArrived
        }
      }
    }
  }

  def confirmRemove(xid: Int): Unit = {
    synchronized {
      if (!closed) {
        if (config.keepJournal) journal.confirmRemove(xid)
        openTransactions.remove(xid)
      }
    }
  }

  def flush(): Unit = {
    while (remove(false).isDefined) { }
  }

  /**
   * Close the queue's journal file. Not safe to call on an active queue.
   */
  def close(): Unit = synchronized {
    closed = true
    if (config.keepJournal) journal.close()
    for (w <- waiters) {
      w.actor ! ItemArrived
    }
    waiters.clear()
  }

  def pauseReads(): Unit = synchronized {
    paused = true
    for (w <- waiters) {
      w.actor ! ItemArrived
    }
    waiters.clear()
  }

  def resumeReads(): Unit = synchronized {
    paused = false
  }

  def setup(): Unit = synchronized {
    queueSize = 0
    replayJournal
  }

  def destroyJournal(): Unit = synchronized {
    if (config.keepJournal) journal.erase()
  }

  private final def nextXid(): Int = {
    do {
      xidCounter += 1
    } while (openTransactions contains xidCounter)
    xidCounter
  }

  private final def fillReadBehind(): Unit = {
    // if we're in read-behind mode, scan forward in the journal to keep memory as full as
    // possible. this amortizes the disk overhead across all reads.
    while (config.keepJournal && journal.inReadBehind && _memoryBytes < config.maxMemorySize.inBytes) {
      journal.fillReadBehind { item =>
        queue += item
        _memoryBytes += item.data.length
      }
      if (!journal.inReadBehind) {
        log.info("Coming out of read-behind for queue '%s'", name)
      }
    }
  }

  def replayJournal(): Unit = {
    if (!config.keepJournal) return

    log.info("Replaying transaction journal for '%s'", name)
    xidCounter = 0

    journal.replay(name) {
      case JournalItem.Add(item) =>
        _add(item)
        // when processing the journal, this has to happen after:
        if (!journal.inReadBehind && queueSize >= config.maxMemorySize.inBytes) {
          log.info("Dropping to read-behind for queue '%s' (%d bytes)", name, queueSize)
          journal.startReadBehind
        }
      case JournalItem.Remove => _remove(false)
      case JournalItem.RemoveTentative => _remove(true)
      case JournalItem.SavedXid(xid) => xidCounter = xid
      case JournalItem.Unremove(xid) => _unremove(xid)
      case JournalItem.ConfirmRemove(xid) => openTransactions.remove(xid)
      case JournalItem.Continue(item, xid) =>
        openTransactions.remove(xid)
        _add(item)
      case x => log.error("Unexpected item in journal: %s", x)
    }

    log.info("Finished transaction journal for '%s' (%d items, %d bytes)", name, queueLength,
             journal.size)
    journal.open

    // now, any unfinished transactions must be backed out.
    for (xid <- openTransactionIds) {
      journal.unremove(xid)
      _unremove(xid)
    }
  }


  //  -----  internal implementations

  private def _add(item: QItem): Unit = {
    discardExpired(config.maxExpireSweep)
    if (!journal.inReadBehind) {
      queue += item
      _memoryBytes += item.data.length
    }
    totalItems.incr()
    queueSize += item.data.length
    queueLength += 1
  }

  private def _peek(): Option[QItem] = {
    discardExpired(config.maxExpireSweep)
    if (queue.isEmpty) None else Some(queue.front)
  }

  private def _remove(transaction: Boolean): Option[QItem] = {
    discardExpired(config.maxExpireSweep)
    if (queue.isEmpty) return None

    val now = Time.now
    val item = queue.dequeue
    val len = item.data.length
    queueSize -= len
    _memoryBytes -= len
    queueLength -= 1
    val xid = if (transaction) nextXid else 0

    fillReadBehind
    _currentAge = now - item.addTime
    if (transaction) {
      item.xid = xid
      openTransactions(xid) = item
    }
    Some(item)
  }

  final def discardExpired(max: Int): Int = {
    if (max > 0) {
      var count = 0
      val itemsToRemove = synchronized {
        var continue = true
        val toRemove = new mutable.ListBuffer[QItem]
        while (continue) {
          if (queue.isEmpty || journal.isReplaying) {
            continue = false
          } else {
            val realExpiry = adjustExpiry(queue.front.addTime, queue.front.expiry)
            if (realExpiry.isDefined && realExpiry.get < Time.now && count < max) {
              totalExpired.incr()
              val item = queue.dequeue
              val len = item.data.length
              queueSize -= len
              _memoryBytes -= len
              queueLength -= 1
              fillReadBehind
              if (config.keepJournal) journal.remove()
              toRemove += item
              count += 1
            } else {
              continue = false
            }
          }
        }
        toRemove
      }

      expireQueue.foreach { q =>
        itemsToRemove.foreach { item => q.add(item.data, None) }
      }
      itemsToRemove.size
    } else 0
  }

  private def _unremove(xid: Int) = {
    openTransactions.remove(xid) map { item =>
      queueLength += 1
      queueSize += item.data.length
      item +=: queue
      _memoryBytes += item.data.length
    }
  }
}
