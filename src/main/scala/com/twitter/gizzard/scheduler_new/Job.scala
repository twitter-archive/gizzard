package com.twitter.gizzard.scheduler

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
