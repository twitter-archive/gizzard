package com.twitter.gizzard
package scheduler

import com.twitter.util.Duration

/**
 * Anything that can receive jobs.
 */
trait JobConsumer[J <: Job] {
  def put(job: J)
}

/**
 * A promise to work on a job. For journaled queues, a ticket is a "tentative get" that isn't
 * confirmed until the 'ack' method is called. If a ticket isn't acked before the server shuts
 * down or dies, the job will be retried on the next startup.
 */
trait Ticket[J <: Job] {
  def job: J
  def ack()
}

/**
 * A job queue that can put and get, and be drained into another queue.
 */
trait JobQueue[J <: Job] extends JobConsumer[J] with Process {
  def get(): Option[Ticket[J]]
  def drainTo(queue: JobQueue[J], delay: Duration)
  def checkExpiration(flushLimit: Int)
  def size: Int
}

/**
 * A mechanism for turning jobs into byte arrays (and vice versa) so that they can be used with
 * journaled queues.
 */
trait Codec[J <: Job] {
  def flatten(job: J): Array[Byte]
  def inflate(data: Array[Byte]): J
}
