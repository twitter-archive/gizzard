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

import scala.collection.Map
import scala.collection.mutable
import net.lag.configgy.ConfigMap

object PrioritizingJobScheduler {
  def apply[J <: Job](config: ConfigMap, codec: Codec[J], queueNames: Map[Int, String],
                      badJobQueue: Option[JobConsumer[J]]) = {
    val schedulerMap = new mutable.HashMap[Int, JobScheduler[J]]
    queueNames.foreach { case (priority, queueName) =>
      schedulerMap(priority) = JobScheduler(queueName, config, codec, badJobQueue)
    }
    new PrioritizingJobScheduler[J](schedulerMap)
  }
}

/**
 * A map of JobSchedulers by priority. It can be treated as a single scheduler, and all process
 * operations work on the cluster of schedulers as a whole.
 */
class PrioritizingJobScheduler[J <: Job](val _schedulers: Map[Int, JobScheduler[J]]) extends Process {
  val schedulers = mutable.Map.empty[Int, JobScheduler[J]] ++ _schedulers

  def put(priority: Int, job: J) {
    apply(priority).put(job)
  }

  def apply(priority: Int): JobScheduler[J] = {
    schedulers.get(priority).getOrElse {
      throw new Exception("No scheduler for priority " + priority)
    }
  }

  def update(priority: Int, scheduler: JobScheduler[J]) {
    schedulers(priority) = scheduler
  }

  def start() = schedulers.values.foreach { _.start() }
  def shutdown() = schedulers.values.foreach { _.shutdown() }
  def isShutdown = schedulers.values.forall { _.isShutdown }
  def pause() = schedulers.values.foreach { _.pause() }
  def resume() = schedulers.values.foreach { _.resume() }
  def retryErrors() = schedulers.values.foreach { _.retryErrors() }

  def size = schedulers.values.foldLeft(0) { _ + _.size }
}
