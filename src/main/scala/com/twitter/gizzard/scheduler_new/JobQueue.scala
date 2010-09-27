package com.twitter.gizzard.scheduler_new

trait JobConsumer[J <: Job[_]] {
  def put(job: J)
}

trait Ticket[J <: Job[_]] {
  def job: J
  def ack()
}

trait JobQueue[J <: Job[_]] extends JobConsumer[J] with Process {
  def get(): Option[Ticket[J]]
  def drainTo(queue: JobQueue[J])
  def size: Int
}

trait Codec[J <: Job[_]] {
  def flatten(job: J): Array[Byte]
  def inflate(data: Array[Byte]): J
}
