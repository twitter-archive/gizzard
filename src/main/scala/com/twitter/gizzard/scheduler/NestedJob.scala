/*
 * Copyright 2010 Twitter, Inc.
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

package com.twitter.gizzard.scheduler

import scala.collection.mutable

/**
 * A wrapper Job for a series of smaller jobs that should be executed together in series.
 * If any of the smaller jobs throws an exception, the NestedJob is enqueued with only that
 * job and the remaining jobs -- in other words, it's enqueued with its progress so far.
 */
class NestedJob[J <: Job](val jobs: Iterable[J]) extends Job {
  val taskQueue = {
    val q = new mutable.Queue[J]()
    q ++= jobs
    q
  }

  def apply() {
    while (!taskQueue.isEmpty) {
      taskQueue.first.apply()
      taskQueue.dequeue()
    }
  }

  override def loggingName = jobs.map { _.loggingName }.mkString(",")

  override def equals(other: Any) = {
    other match {
      case other: NestedJob[_] if (other ne null) =>
        taskQueue.toList == other.taskQueue.toList
      case _ =>
        false
    }
  }

  override def toString = "<NestedJob: tasks=%d: %s>".format(taskQueue.size, jobs)
}
