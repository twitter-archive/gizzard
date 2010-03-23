package com.twitter.gizzard.scheduler

import scala.collection.mutable


trait MessageQueue[Serializable, Argumentable] extends Collection[Argumentable] with Scheduler[Serializable] with Process {
  def writeTo[A](messageQueue: MessageQueue[Serializable, A])
}
