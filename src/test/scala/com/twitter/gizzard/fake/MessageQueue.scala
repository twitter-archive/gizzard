package com.twitter.gizzard.fake

import java.util.concurrent.CountDownLatch
import jobs.{Schedulable, BoundJob}


class MessageQueue extends scheduler.MessageQueue[Schedulable, jobs.Job] {
  var latch: CountDownLatch = null
  var isShutdown = true

  def put(serializable: Schedulable) = ()

  def start() {
    latch = new CountDownLatch(1)
    isShutdown = false
  }

  def resume() = start()

  def pause() = shutdown()

  def shutdown() {
    latch.countDown()
    isShutdown = true
  }

  def size = 0

  def elements = new Iterator[jobs.Job] {
    def hasNext = !isShutdown
    def next = {
      latch.await()
      new BoundJob(new Job(Map()), 1)
    }
  }

  override def toString = "<WarbleGarble>"

  def writeTo[A](messageQueue: scheduler.MessageQueue[Schedulable, A]) = ()
  def writeTo[A](messageQueue: scheduler.MessageQueue[Schedulable, A], i: Int) = ()
}
  