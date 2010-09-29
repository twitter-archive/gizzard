package com.twitter.gizzard.scheduler

/**
 * A unit of work that can be queued for later. The work is executed by calling 'apply', and if
 * an exception is thrown, the job's error count is incremented and it is enqueued on a separate
 * error queue.
 */
trait Job {
  @throws(classOf[Exception])
  def apply(): Unit

  var errorCount: Int = 0
  var errorMessage: String = "(none)"

  def loggingName = {
    val className = getClass.getName
    className.lastIndexOf('.') match {
      case -1 => className
      case n => className.substring(n + 1)
    }
  }
}
