package com.twitter.gizzard

import scala.collection.mutable
import net.lag.logging.Logger
import jobs.{Schedulable, Job}


trait MessageQueue {
  private val log = Logger.get(getClass.getName)

  def put(value: Schedulable)

  def pause()

  def resume()

  def shutdown()

  def isShutdown: Boolean

  def size: Int

  def writeTo(messageQueue: MessageQueue)

  def foreach(blocking: Boolean, f: Job => Unit): Unit

  def map[T](f: Job => T): Seq[T] = {
    val rv = new mutable.ListBuffer[T]
    foreach(false, { job => rv += f(job) })
    rv
  }
}
