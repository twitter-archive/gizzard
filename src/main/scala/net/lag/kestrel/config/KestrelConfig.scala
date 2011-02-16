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

package net.lag.kestrel.config

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.util.{Duration, StorageUnit}

case class QueueConfig(
  maxItems: Int,
  maxSize: StorageUnit,
  maxItemSize: StorageUnit,
  maxAge: Option[Duration],
  maxJournalSize: StorageUnit,
  maxMemorySize: StorageUnit,
  maxJournalOverflow: Int,
  discardOldWhenFull: Boolean,
  keepJournal: Boolean,
  syncJournal: Boolean,
  multifileJournal: Boolean,
  expireToQueue: Option[String],
  maxExpireSweep: Int,
  fanoutOnly: Boolean
) {
  override def toString() = {
    ("maxItems=%d maxSize=%s maxItemSize=%s maxAge=%s maxJournalSize=%s maxMemorySize=%s " +
     "maxJournalOverflow=%d discardOldWhenFull=%s keepJournal=%s syncJournal=%s " +
     "mutlifileJournal=%s expireToQueue=%s maxExpireSweep=%d fanoutOnly=%s").format(maxItems, maxSize,
     maxItemSize, maxAge, maxJournalSize, maxMemorySize, maxJournalOverflow, discardOldWhenFull,
     keepJournal, syncJournal, multifileJournal, expireToQueue, maxExpireSweep, fanoutOnly)
  }
}
