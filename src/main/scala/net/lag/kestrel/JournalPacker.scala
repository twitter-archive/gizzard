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

import java.io.FileOutputStream
import scala.collection.mutable
import com.twitter.logging.Logger

/**
 * Pack one or more journal files into a single new file that only consists of the queue's current
 * contents, as of the end of the last journal file processed.
 */
class JournalPacker(filenames: Seq[String], newFilename: String) {
  private val log = Logger.get

  val journals = filenames.map { filename => new Journal(filename, false) }
  val remover = journals.map { _.walk() }.iterator.flatten
  val adder = journals.map { _.walk() }.iterator.flatten
  val writer = new FileOutputStream(newFilename, false).getChannel

  val adderStack = new mutable.ListBuffer[QItem]
  val openTransactions = new mutable.HashMap[Int, QItem]
  var currentXid = 0

  var offset = 0L
  var adderOffset = 0L
  var lastUpdate = 0L
  var lastAdderUpdate = 0L

  private var statusCallback: ((Long, Long) => Unit) = (_, _) => ()

  private def advanceAdder(): Option[QItem] = {
    if (!adderStack.isEmpty) {
      Some(adderStack.remove(0))
    } else {
      if (!adder.hasNext) {
        None
      } else {
        val (item, itemsize) = adder.next()
        adderOffset += itemsize
        if (adderOffset - lastAdderUpdate > 1024 * 1024) {
          statusCallback(offset, adderOffset)
          lastAdderUpdate = adderOffset
        }
        item match {
          case JournalItem.Add(qitem) => Some(qitem)
          case _ => advanceAdder()
        }
      }
    }
  }

  def apply(statusCallback: (Long, Long) => Unit) = {
    this.statusCallback = statusCallback
    for ((item, itemsize) <- remover) {
      item match {
        case JournalItem.Add(qitem) =>
        case JournalItem.Continue(qitem, xid) =>
          openTransactions -= xid
        case JournalItem.Remove =>
          advanceAdder().get
        case JournalItem.RemoveTentative =>
          do {
            currentXid += 1
          } while (openTransactions contains currentXid)
          val qitem = advanceAdder().get
          qitem.xid = currentXid
          openTransactions(currentXid) = qitem
        case JournalItem.SavedXid(xid) =>
          currentXid = xid
        case JournalItem.Unremove(xid) =>
          adderStack prepend openTransactions.remove(xid).get
        case JournalItem.ConfirmRemove(xid) =>
          openTransactions -= xid
      }
      offset += itemsize
      if (offset - lastUpdate > 1024 * 1024) {
        statusCallback(offset, adderOffset)
        lastUpdate = offset
      }
    }

    // now write the new journal.
    statusCallback(0, 0)
    val remaining = new Iterable[QItem] {
      def iterator = new tools.PythonIterator[QItem] {
        def apply() = advanceAdder()
      }
    }

    val out = new Journal(newFilename, false)
    out.open()
    out.dump(currentXid, openTransactions.values.toList, remaining)
    out.close()
    out
  }
}
