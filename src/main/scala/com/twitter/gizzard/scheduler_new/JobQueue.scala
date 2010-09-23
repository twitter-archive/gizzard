package com.twitter.gizzard.scheduler_new

trait JobConsumer[E, J <: Job[E]] {
  def put(job: J)
}

trait Ticket[E, J <: Job[E]] {
  def job: J
  def ack()
}

trait JobQueue[E, J <: Job[E]] extends JobConsumer[E, J] with Process {
  def get(): Option[Ticket[E, J]]
  def drainTo(queue: JobQueue[E, J])
}

trait Codec[E, J <: Job[E]] {
  def flatten(job: J): Array[Byte]
  def inflate(data: Array[Byte]): J
}
