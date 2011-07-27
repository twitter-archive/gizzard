package com.twitter.gizzard
package scheduler

import com.twitter.util.Duration
import com.twitter.gizzard.util.Process


/**
 * Anything that can receive jobs.
 */
trait JobConsumer {
  def put(job: JsonJob)
}

/**
 * A promise to work on a job. For journaled queues, a ticket is a "tentative get" that isn't
 * confirmed until the 'ack' method is called. If a ticket isn't acked before the server shuts
 * down or dies, the job will be retried on the next startup.
 */
trait Ticket {
  def job: JsonJob
  def ack()
  def continue(job: JsonJob)
}

/**
 * A job queue that can put and get, and be drained into another queue.
 */
trait JobQueue extends JobConsumer with Process {
  def get(): Option[Ticket]
  def drainTo(queue: JobQueue, delay: Duration)
  def checkExpiration(flushLimit: Int)
  def size: Int
}
