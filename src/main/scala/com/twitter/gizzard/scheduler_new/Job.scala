package com.twitter.gizzard.scheduler_new

abstract class Job[E](val environment: E) extends (E => Unit) {
  var errorCount: Int = 0
  var errorMessage: String = "(none)"

  @throws(classOf[Exception])
  def apply(): Unit = apply(environment)

  @throws(classOf[Exception])
  def apply(environment: E): Unit

  def loggingName = {
    val className = getClass.getName
    className.lastIndexOf('.') match {
      case -1 => className
      case n => className.substring(n + 1)
    }
  }
}
