package com.twitter.gizzard.scheduler

trait JobConsumer[J <: Job] {
  def put(job: J)
}

trait Ticket[J <: Job] {
  def job: J
  def ack()
}

trait JobQueue[J <: Job] extends JobConsumer[J] with Process {
  def get(): Option[Ticket[J]]
  def drainTo(queue: JobQueue[J])
  def size: Int
}

trait Codec[J <: Job] {
  def flatten(job: J): Array[Byte]
  def inflate(data: Array[Byte]): J
}
